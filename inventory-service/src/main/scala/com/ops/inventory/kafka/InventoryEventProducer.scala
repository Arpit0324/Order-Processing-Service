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
  private val producer = SendProducer(producerSettings)

  // All items reserved successfully → order service can confirm
  def publishAllReserved(orderId: String, items: List[ItemLine], traceId: String): Future[Unit] =
    Future.traverse(items) { item =>
      send("inventory.updated", item.productId, InventoryUpdatedEvent(
        eventId      = UUID.randomUUID().toString,
        occurredAt   = Instant.now(),
        traceId      = traceId,
        orderId      = orderId,
        productId    = item.productId,
        reservedQty  = item.quantity,
        remainingQty = 0,    // actual remaining is in DB; 0 is a safe default for consumers
        status       = "RESERVED"
      ).asJson.noSpaces)
    }.map(_ => ())

  // Reservation failed → trigger saga compensation in Order Service
  def publishReservationFailed(orderId: String, failedProducts: List[String], traceId: String): Future[Unit] =
    send("order.cancel.requested", orderId, OrderCancelRequestedEvent(
      eventId        = UUID.randomUUID().toString,
      occurredAt     = Instant.now(),
      traceId        = traceId,
      orderId        = orderId,
      reason         = "INSUFFICIENT_STOCK",
      failedProducts = failedProducts
    ).asJson.noSpaces)

  def publishReleased(orderId: String, traceId: String): Future[Unit] =
    send("inventory.updated", orderId, InventoryUpdatedEvent(
      eventId      = UUID.randomUUID().toString,
      occurredAt   = Instant.now(),
      traceId      = traceId,
      orderId      = orderId,
      productId    = "",
      reservedQty  = 0,
      remainingQty = 0,
      status       = "RELEASED"
    ).asJson.noSpaces)

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
