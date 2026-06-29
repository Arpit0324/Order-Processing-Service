package com.ops.order.service

import akka.actor.typed.{ActorRef, ActorSystem}
import akka.actor.typed.scaladsl.AskPattern.*
import akka.util.Timeout
import com.ops.order.actor.OrderSupervisor
import com.ops.order.api.dto.*
import com.ops.order.domain.*
import com.ops.order.kafka.OrderEventProducer
import com.ops.order.repository.OrderRepository
import org.slf4j.LoggerFactory
import java.util.UUID
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.*

class OrderServiceImpl(
  supervisor: ActorRef[OrderSupervisor.ForwardToOrder],
  repository: OrderRepository,
  producer:   OrderEventProducer
)(using system: ActorSystem[?], ec: ExecutionContext) extends OrderService {

  private val log = LoggerFactory.getLogger(getClass)
  private given Timeout = Timeout(5.seconds)

  // ── Create order ─────────────────────────────────────────────────────────────
  override def createOrder(req: CreateOrderRequest, traceId: String): Future[Either[ServiceError, OrderResponse]] = {
    val orderId = UUID.randomUUID().toString
    val cmd = CreateOrder(
      orderId         = orderId,
      customerId      = req.customerId,
      items           = req.items,
      shippingAddress = req.shippingAddress,
      totalAmount     = req.items.map(i => i.unitPrice * i.quantity).sum,
      idempotencyKey  = req.idempotencyKey,
      replyTo         = _
    )

    ask(cmd).flatMap {
      case OrderCreatedReply(order) =>
        // Persist to DB (read model) + publish Kafka event
        repository.save(order)
          .flatMap(_ => producer.publishOrderCreated(order, traceId))
          .map(_ => Right(OrderResponse.from(order)))
          .recover { case ex =>
            log.error("Post-persist failure for orderId={}", orderId, ex)
            Left(InternalError(ex.getMessage))
          }
      case OrderRejected(reason)    => Future.successful(Left(InvalidTransition(reason)))
      case _                        => Future.successful(Left(InternalError("Unexpected actor reply")))
    }
  }

  // ── Get order ────────────────────────────────────────────────────────────────
  override def getOrder(id: String): Future[Option[OrderResponse]] =
    repository.findById(id).map(_.map(OrderResponse.from))

  // ── List orders ──────────────────────────────────────────────────────────────
  override def listOrders(customerId: String, page: Int, pageSize: Int): Future[OrderListResponse] =
    repository.findByCustomer(customerId, page, pageSize)
      .map { case (orders, total) =>
        OrderListResponse(
          orders   = orders.map(OrderResponse.from),
          total    = total,
          page     = page,
          pageSize = pageSize
        )
      }

  // ── Cancel order ─────────────────────────────────────────────────────────────
  override def cancelOrder(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]] =
    sendAndSync(CancelOrder(id, reason, _)) { order =>
      producer.publishOrderCancelled(order, traceId)
    }

  // ── Fulfill order ─────────────────────────────────────────────────────────────
  override def fulfillOrder(id: String, traceId: String): Future[Either[ServiceError, OrderResponse]] =
    sendAndSync(FulfillOrder(id, _)) { _ => Future.unit }

  // ── Return lifecycle ──────────────────────────────────────────────────────────
  override def requestReturn(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]] =
    repository.findById(id).flatMap {
      case None        => Future.successful(Left(NotFound(id)))
      case Some(order) =>
        sendAndSync(RequestReturn(id, reason, order.items, _)) { _ => Future.unit }
    }

  override def approveReturn(id: String, traceId: String): Future[Either[ServiceError, OrderResponse]] =
    sendAndSync(ApproveReturn(id, _)) { order =>
      producer.publishOrderReturned(order, traceId)
    }

  override def rejectReturn(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]] =
    sendAndSync(RejectReturn(id, reason, _)) { _ => Future.unit }

  // ── Shared: ask actor, sync DB, run side-effect ───────────────────────────────
  private def sendAndSync(
    mkCmd: ActorRef[OrderReply] => OrderCommand
  )(sideEffect: Order => Future[Unit]): Future[Either[ServiceError, OrderResponse]] =
    ask(mkCmd).flatMap {
      case OrderUpdatedReply(order) =>
        repository.updateStatus(order.id, order.status)
          .flatMap(_ => sideEffect(order))
          .map(_ => Right(OrderResponse.from(order)))
          .recover { case ex => Left(InternalError(ex.getMessage)) }
      case OrderNotFound(id)        => Future.successful(Left(NotFound(id)))
      case OrderRejected(reason)    => Future.successful(Left(InvalidTransition(reason)))
      case _                        => Future.successful(Left(InternalError("Unexpected actor reply")))
    }

  private def ask(mkCmd: ActorRef[OrderReply] => OrderCommand): Future[OrderReply] =
    supervisor.ask(ref => OrderSupervisor.ForwardToOrder(mkCmd(ref)))
}
