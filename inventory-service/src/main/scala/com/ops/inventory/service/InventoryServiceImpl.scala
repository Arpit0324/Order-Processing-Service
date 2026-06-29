package com.ops.inventory.service

import com.ops.inventory.api.dto.InventoryResponse
import com.ops.inventory.domain.{InventoryItem, Reserved, ReservationFailed}
import com.ops.inventory.kafka.InventoryEventProducer
import com.ops.inventory.repository.InventoryRepository
import com.ops.shared.events.OrderCreatedEvent
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}

class InventoryServiceImpl(
  repository: InventoryRepository,
  producer:   InventoryEventProducer
)(using ec: ExecutionContext) extends InventoryService {

  private val log = LoggerFactory.getLogger(getClass)
  private val MaxLockRetries = 3

  override def getItem(productId: String): Future[Option[InventoryResponse]] =
    repository.findById(productId).map(_.map(InventoryResponse.from))

  override def listItems(page: Int, pageSize: Int): Future[(List[InventoryResponse], Int)] =
    repository.findAll(page, pageSize).map { case (items, total) =>
      (items.map(InventoryResponse.from), total)
    }

  // ── Core: reserve stock for every item in the order ──────────────────────────
  // For each item: fetch → check → reserve with optimistic lock → retry on conflict
  // If ANY item fails: publish order.cancel.requested (saga compensation)
  override def reserveForOrder(event: OrderCreatedEvent, traceId: String): Future[ReservationOutcome] = {
    Future.traverse(event.items) { itemLine =>
      reserveWithRetry(event.orderId, itemLine.productId, itemLine.quantity, traceId, retries = MaxLockRetries)
    }.flatMap { results =>
      val failures = results.collect { case f: ReservationFailed => f.productId }
      if (failures.isEmpty) {
        producer.publishAllReserved(event.orderId, event.items, traceId).map(_ => AllReserved(event.orderId))
      } else {
        // Rollback already-reserved items for this order
        repository.releaseReservation(event.orderId).flatMap { _ =>
          producer.publishReservationFailed(event.orderId, failures, traceId)
            .map(_ => PartiallyFailed(event.orderId, failures))
        }
      }
    }
  }

  override def releaseForOrder(orderId: String, traceId: String): Future[Unit] =
    repository.releaseReservation(orderId)
      .flatMap(_ => producer.publishReleased(orderId, traceId))

  override def commitForOrder(orderId: String): Future[Unit] =
    repository.commitReservation(orderId)

  override def restockFromReturn(productId: String, qty: Int, traceId: String): Future[Option[InventoryResponse]] =
    repository.restockFromReturn(productId, qty)
      .flatMap { result =>
        producer.publishRestocked(productId, qty, traceId).map(_ => result.map(InventoryResponse.from))
      }

  override def updateStock(productId: String, delta: Int, traceId: String): Future[Option[InventoryResponse]] =
    repository.updateQuantity(productId, delta).map(_.map(InventoryResponse.from))

  // ── Optimistic lock retry ────────────────────────────────────────────────────
  private def reserveWithRetry(orderId: String, productId: String, qty: Int,
                                traceId: String, retries: Int): Future[Any] = {
    repository.findById(productId).flatMap {
      case None =>
        Future.successful(ReservationFailed(productId, "PRODUCT_NOT_FOUND", 0))
      case Some(item) if !item.canReserve(qty) =>
        log.warn("Insufficient stock for productId={} requested={} available={}",
                 productId, qty, item.availableQty)
        Future.successful(ReservationFailed(productId, "INSUFFICIENT_STOCK", item.availableQty))
      case Some(item) =>
        repository.reserveWithLock(productId, orderId, qty, item.version).flatMap {
          case Right(updated) =>
            Future.successful(Reserved(productId, qty, updated.availableQty))
          case Left("LOCK_CONFLICT_OR_INSUFFICIENT_STOCK") if retries > 0 =>
            log.debug("Optimistic lock conflict for productId={}, retrying ({} left)", productId, retries)
            reserveWithRetry(orderId, productId, qty, traceId, retries - 1)
          case Left(reason) =>
            Future.successful(ReservationFailed(productId, reason, 0))
        }
    }
  }
}
