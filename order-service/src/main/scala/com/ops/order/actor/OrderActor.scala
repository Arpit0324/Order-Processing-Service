package com.ops.order.actor

import org.apache.pekko.actor.typed.{ActorRef, Behavior, SupervisorStrategy}
import org.apache.pekko.actor.typed.scaladsl.{ActorContext, Behaviors, TimerScheduler}
import org.apache.pekko.persistence.typed.PersistenceId
import org.apache.pekko.persistence.typed.scaladsl.{Effect, EventSourcedBehavior, RetentionCriteria}
import com.ops.order.domain.*
import com.ops.shared.domain.ItemLine
import java.time.Instant
import scala.concurrent.duration.*

// ── OrderActor — one actor per orderId ────────────────────────────────────────
// Uses Akka Typed EventSourcedBehavior:
//   Command → validate against state → persist Event → apply to state → reply
// On pod restart: Akka replays all persisted events to rebuild state in-memory.
// Snapshot every 20 events to keep replay fast.
object OrderActor {

  // SLA timeout: cancel PENDING orders that never get confirmed
  private val SlaPendingTimeout = 15.minutes
  private case object SlaTimerKey

  def apply(orderId: String): Behavior[OrderCommand] =
    Behaviors.withTimers { timers =>
      Behaviors.supervise(
        eventSourcedBehavior(orderId, timers)
      ).onFailure[Exception](
        SupervisorStrategy.restartWithBackoff(minBackoff = 1.second, maxBackoff = 30.seconds, randomFactor = 0.2)
      )
    }

  private def eventSourcedBehavior(
    orderId: String,
    timers:  TimerScheduler[OrderCommand]
  ): Behavior[OrderCommand] =
    EventSourcedBehavior[OrderCommand, OrderEvent, OrderState](
      persistenceId  = PersistenceId.ofUniqueId(s"order|$orderId"),
      emptyState     = OrderState.empty,
      commandHandler = commandHandler(orderId, timers),
      eventHandler   = eventHandler
    )
    // Snapshot every 20 events → limits journal replay on restart
    .withRetention(RetentionCriteria.snapshotEvery(numberOfEvents = 20, keepNSnapshots = 2))
    // Jackson JSON serialization (configured in application.conf)
    .withTagger(_ => Set("order"))

  // ── Command Handler: validate → Effect.persist / Effect.reply(error) ────────
  private def commandHandler(
    orderId: String,
    timers:  TimerScheduler[OrderCommand]
  )(state: OrderState, command: OrderCommand): Effect[OrderEvent, OrderState] =
    command match {

      case cmd: CreateOrder =>
        if (state.order.isDefined)
          Effect.reply(cmd.replyTo)(OrderRejected(s"Order $orderId already exists"))
        else
          Effect
            .persist(OrderCreated(
              orderId         = cmd.orderId,
              customerId      = cmd.customerId,
              items           = cmd.items,
              shippingAddress = cmd.shippingAddress,
              totalAmount     = cmd.totalAmount,
              idempotencyKey  = cmd.idempotencyKey,
              at              = Instant.now()
            ))
            .thenRun { (_: OrderState) =>
              // Start SLA timer — fires SlaTimeout if still PENDING after 15 min
              timers.startSingleTimer(SlaTimerKey, SlaTimeout(orderId, cmd.replyTo), SlaPendingTimeout)
            }
            .thenReply(cmd.replyTo)(s => OrderCreatedReply(s.order.get))

      case cmd: ConfirmOrder =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if o.status != OrderStatus.Pending =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Cannot confirm order in status ${OrderStatus.asString(o.status)}"))
          case _ =>
            Effect
              .persist(OrderConfirmed(cmd.orderId, Instant.now()))
              .thenRun { (_: OrderState) => timers.cancel(SlaTimerKey) }
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: CancelOrder =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if o.isTerminal =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Cannot cancel terminal order (${OrderStatus.asString(o.status)})"))
          case Some(o) if !o.canCancel =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Cannot cancel order in status ${OrderStatus.asString(o.status)}"))
          case Some(o) =>
            Effect
              .persist(OrderCancelled(cmd.orderId, cmd.reason, wasConfirmed = o.status == OrderStatus.Confirmed, Instant.now()))
              .thenRun { (_: OrderState) => timers.cancel(SlaTimerKey) }
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: SlaTimeout =>
        state.order match {
          case Some(o) if o.status == OrderStatus.Pending =>
            Effect.persist(OrderCancelled(cmd.orderId, "SLA_TIMEOUT", wasConfirmed = false, Instant.now()))
              .thenNoReply()
          case _ =>
            Effect.none.thenNoReply() // already transitioned — ignore
        }

      case cmd: FulfillOrder =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if o.status != OrderStatus.Confirmed =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Can only fulfill CONFIRMED orders, current: ${OrderStatus.asString(o.status)}"))
          case _ =>
            Effect
              .persist(OrderFulfilled(cmd.orderId, Instant.now()))
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: RequestReturn =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if !o.canRequestReturn =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Can only return FULFILLED orders, current: ${OrderStatus.asString(o.status)}"))
          case _ =>
            Effect
              .persist(ReturnRequested(cmd.orderId, cmd.reason, cmd.items, Instant.now()))
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: ApproveReturn =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if !o.canApproveReturn =>
            Effect.reply(cmd.replyTo)(OrderRejected(s"Can only approve RETURN_REQUESTED orders"))
          case Some(o) =>
            Effect
              .persist(ReturnApproved(cmd.orderId, refundAmount = o.totalAmount, Instant.now()))
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: RejectReturn =>
        state.order match {
          case None =>
            Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(o) if o.status != OrderStatus.ReturnRequested =>
            Effect.reply(cmd.replyTo)(OrderRejected("No pending return request to reject"))
          case _ =>
            Effect
              .persist(ReturnRejected(cmd.orderId, cmd.reason, Instant.now()))
              .thenReply(cmd.replyTo)(s => OrderUpdatedReply(s.order.get))
        }

      case cmd: GetOrder =>
        state.order match {
          case None    => Effect.reply(cmd.replyTo)(OrderNotFound(cmd.orderId))
          case Some(_) => Effect.reply(cmd.replyTo)(OrderDetailsReply(state.order.get))
        }
    }

  // ── Event Handler: apply persisted event → new state (pure function) ────────
  private val eventHandler: (OrderState, OrderEvent) => OrderState = {
    (state, event) => state.applyEvent(event)
  }
}

// ── OrderState — in-memory state rebuilt from event replay ────────────────────
final case class OrderState(order: Option[Order]) {

  def applyEvent(event: OrderEvent): OrderState = event match {
    case e: OrderCreated =>
      copy(order = Some(Order(
        id              = e.orderId,
        customerId      = e.customerId,
        items           = e.items,
        status          = OrderStatus.Pending,
        totalAmount     = e.totalAmount,
        shippingAddress = e.shippingAddress,
        idempotencyKey  = e.idempotencyKey,
        createdAt       = e.at,
        updatedAt       = e.at
      )))

    case e: OrderConfirmed =>
      copy(order = order.map(_.copy(status = OrderStatus.Confirmed, updatedAt = e.at)))

    case e: OrderCancelled =>
      copy(order = order.map(_.copy(
        status          = OrderStatus.Cancelled,
        cancelledReason = Some(e.reason),
        cancelledAt     = Some(e.at),
        updatedAt       = e.at
      )))

    case e: OrderFulfilled =>
      copy(order = order.map(_.copy(status = OrderStatus.Fulfilled, updatedAt = e.at)))

    case e: ReturnRequested =>
      copy(order = order.map(_.copy(
        status            = OrderStatus.ReturnRequested,
        returnRequestedAt = Some(e.at),
        returnReason      = Some(e.reason),
        updatedAt         = e.at
      )))

    case e: ReturnApproved =>
      copy(order = order.map(_.copy(
        status       = OrderStatus.Returned,
        returnedAt   = Some(e.at),
        refundAmount = Some(e.refundAmount),
        updatedAt    = e.at
      )))

    case e: ReturnRejected =>
      // Return rejected → goes back to FULFILLED
      copy(order = order.map(_.copy(status = OrderStatus.Fulfilled, updatedAt = e.at)))
  }
}

object OrderState {
  val empty: OrderState = OrderState(order = None)
}
