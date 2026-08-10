package com.ops.inventory.kafka

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer
import com.ops.shared.domain.ItemLine
import com.ops.shared.events.{InventoryUpdatedEvent, OrderCancelRequestedEvent}
import com.ops.shared.serialization.JsonCodecs.given
import io.circe.syntax.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import java.time.Instant
import java.util.UUID

class InventoryEventProducer()(using system: ActorSystem[?], ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(system.settings.config.getString("kafka.bootstrap-servers"))
    // Fix: ensure-once delivery on producer side
    .withProperty("acks", "all")
    .withProperty("enable.idempotence", "true")
    .withProperty("retries", "3")

  private val producer: SendProducer[String, String] = SendProducer(producerSettings)

  // Fix K: accept remainingQty per item so we don't hardcode 0
  def publishAllReserved(orderId: String, items: List[ItemLine], traceId: String,
                          remainingQtyByProduct: Map[String, Int] = Map.empty): Future[Unit] =
    Future.traverse(items) { item =>
      send("inventory.updated", item.productId, InventoryUpdatedEvent(
        eventId      = UUID.randomUUID().toString,
        occurredAt   = Instant.now(),
        traceId      = traceId,
        orderId      = orderId,
        productId    = item.productId,
        reservedQty  = item.quantity,
        remainingQty = remainingQtyByProduct.getOrElse(item.productId, 0),
        status       = "RESERVED"
      ).asJson.noSpaces)
    }.map(_ => ())

  def publishReservationFailed(orderId: String, failedProducts: List[String], traceId: String): Future[Unit] =
    send("order.cancel.requested", orderId, OrderCancelRequestedEvent(
      eventId        = UUID.randomUUID().toString,
      occurredAt     = Instant.now(),
      traceId        = traceId,
      orderId        = orderId,
      reason         = "INSUFFICIENT_STOCK",
      failedProducts = failedProducts
    ).asJson.noSpaces)

  // Fix L: emit one event per product with real productId, not a single event with empty productId
  def publishReleased(orderId: String, traceId: String,
                       releasedProducts: List[(String, Int)] = Nil): Future[Unit] =
    if (releasedProducts.isEmpty) {
      // ponytail: fallback when product list unavailable -- emit order-level event
      send("inventory.updated", orderId, InventoryUpdatedEvent(
        eventId      = UUID.randomUUID().toString,
        occurredAt   = Instant.now(),
        traceId      = traceId,
        orderId      = orderId,
        productId    = orderId, // use orderId as key when no product breakdown available
        reservedQty  = 0,
        remainingQty = 0,
        status       = "RELEASED"
      ).asJson.noSpaces)
    } else {
      Future.traverse(releasedProducts) { case (productId, qty) =>
        send("inventory.updated", productId, InventoryUpdatedEvent(
          eventId      = UUID.randomUUID().toString,
          occurredAt   = Instant.now(),
          traceId      = traceId,
          orderId      = orderId,
          productId    = productId,
          reservedQty  = 0,
          remainingQty = qty,
          status       = "RELEASED"
        ).asJson.noSpaces)
      }.map(_ => ())
    }

  def publishRestocked(productId: String, qty: Int, traceId: String): Future[Unit] =
    send("inventory.updated", productId, InventoryUpdatedEvent(
      eventId      = UUID.randomUUID().toString,
      occurredAt   = Instant.now(),
      traceId      = traceId,
      orderId      = "",
      productId    = productId,
      reservedQty  = 0,
      remainingQty = qty,
      status       = "RESTOCKED"
    ).asJson.noSpaces)

  private def send(topic: String, key: String, value: String): Future[Unit] =
    producer.send(new ProducerRecord[String, String](topic, key, value))
      .map(m => log.debug("Published topic={} partition={} offset={}", topic, m.partition(), m.offset()))
      .recover { case ex => log.error("Kafka publish failed topic={} key={}", topic, key, ex); throw ex }
}
