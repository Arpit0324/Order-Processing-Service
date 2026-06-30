package com.ops.order.kafka

import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.kafka.ProducerSettings
import org.apache.pekko.kafka.scaladsl.SendProducer
import com.ops.order.domain.Order
import com.ops.shared.events.*
import com.ops.shared.serialization.JsonCodecs.given
import io.circe.syntax.*
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import java.util.UUID

// ── OrderEventProducer — publishes domain events to Kafka ─────────────────────
// Called from service layer AFTER actor confirms state change.
// Key = orderId → ensures all events for same order go to same partition.
class OrderEventProducer()(using system: ActorSystem[?], ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)

  private val producerSettings: ProducerSettings[String, String] =
    ProducerSettings(system, new StringSerializer, new StringSerializer)
      .withBootstrapServers(system.settings.config.getString("kafka.bootstrap-servers"))

  private val producer = SendProducer(producerSettings)

  def publishOrderCreated(order: Order, traceId: String): Future[Unit] = {
    val event = OrderCreatedEvent(
      eventId         = UUID.randomUUID().toString,
      occurredAt      = order.createdAt,
      traceId         = traceId,
      orderId         = order.id,
      customerId      = order.customerId,
      items           = order.items,
      totalAmount     = order.totalAmount,
      shippingAddress = order.shippingAddress
    )
    send("order.created", order.id, event.asJson.noSpaces)
  }

  def publishOrderCancelled(order: Order, traceId: String): Future[Unit] = {
    val event = OrderCancelledEvent(
      eventId      = UUID.randomUUID().toString,
      occurredAt   = order.updatedAt,
      traceId      = traceId,
      orderId      = order.id,
      customerId   = order.customerId,
      reason       = order.cancelledReason.getOrElse("UNKNOWN"),
      wasConfirmed = order.cancelledReason.exists(_ != "SLA_TIMEOUT"),
      items        = order.items
    )
    send("order.cancelled", order.id, event.asJson.noSpaces)
  }

  def publishOrderReturned(order: Order, traceId: String): Future[Unit] = {
    val event = OrderReturnedEvent(
      eventId      = UUID.randomUUID().toString,
      occurredAt   = order.returnedAt.getOrElse(order.updatedAt),
      traceId      = traceId,
      orderId      = order.id,
      customerId   = order.customerId,
      items        = order.items,
      refundAmount = order.refundAmount.getOrElse(BigDecimal(0))
    )
    send("order.returned", order.id, event.asJson.noSpaces)
      .flatMap(_ => publishRefundRequested(order, traceId))
  }

  private def publishRefundRequested(order: Order, traceId: String): Future[Unit] = {
    val event = RefundRequestedEvent(
      eventId      = UUID.randomUUID().toString,
      occurredAt   = order.updatedAt,
      traceId      = traceId,
      orderId      = order.id,
      customerId   = order.customerId,
      refundAmount = order.refundAmount.getOrElse(BigDecimal(0))
    )
    send("refund.requested", order.id, event.asJson.noSpaces)
  }

  private def send(topic: String, key: String, value: String): Future[Unit] = {
    val record = new ProducerRecord[String, String](topic, key, value)
    producer.send(record)
      .map { metadata =>
        log.debug("Published to topic={} partition={} offset={}", topic, metadata.partition(), metadata.offset())
      }
      .recover { case ex =>
        log.error("Failed to publish to topic={} key={}", topic, key, ex)
        throw ex
      }
  }
}
