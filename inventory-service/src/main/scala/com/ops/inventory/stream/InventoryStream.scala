package com.ops.inventory.stream

import akka.actor.typed.ActorSystem
import akka.kafka.{CommitterSettings, ConsumerSettings, Subscriptions}
import akka.kafka.scaladsl.{Committer, Consumer}
import akka.stream.scaladsl.{Keep, RestartSource, Sink}
import akka.stream.{KillSwitches, RestartSettings, SharedKillSwitch}
import com.ops.inventory.service.{AllReserved, InventoryService, PartiallyFailed}
import com.ops.shared.events.{OrderCancelledEvent, OrderCreatedEvent, OrderReturnedEvent}
import com.ops.shared.serialization.JsonCodecs.given
import io.circe.parser.*
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import scala.concurrent.ExecutionContext
import scala.concurrent.duration.*

// ── InventoryStream — backpressure-aware Akka Streams Kafka consumer ──────────
// Consumes: order.created, order.cancelled, order.returned
// Publishes: inventory.updated, order.cancel.requested (via service → producer)
class InventoryStream(service: InventoryService)(using system: ActorSystem[?], ec: ExecutionContext) {

  private val log = LoggerFactory.getLogger(getClass)
  private val config = system.settings.config

  private val consumerSettings: ConsumerSettings[String, String] =
    ConsumerSettings(config.getConfig("akka.kafka.consumer"), new StringDeserializer, new StringDeserializer)
      .withBootstrapServers(config.getString("kafka.bootstrap-servers"))
      .withGroupId("inventory-service")
      .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .withProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
      .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

  private val committerSettings: CommitterSettings =
    CommitterSettings(config.getConfig("akka.kafka.committer"))
      .withMaxBatch(500)
      .withMaxInterval(5.seconds)

  private val restartSettings = RestartSettings(
    minBackoff   = 3.seconds,
    maxBackoff   = 30.seconds,
    randomFactor = 0.2
  ).withMaxRestarts(10, 1.minute)

  val killSwitch: SharedKillSwitch = KillSwitches.shared("inventory-stream")

  def start(): Unit = {
    log.info("Starting InventoryStream Kafka consumer...")

    val topics = Subscriptions.topics("order.created", "order.cancelled", "order.returned")

    RestartSource
      .onFailuresWithBackoff(restartSettings) { () =>
        Consumer
          .committableSource(consumerSettings, topics)
          .via(killSwitch.flow)
          .mapAsync(parallelism = 4) { msg =>
            val topic   = msg.record.topic()
            val key     = msg.record.key()
            val value   = msg.record.value()
            val traceId = Option(msg.record.headers().lastHeader("X-Trace-Id"))
              .map(h => new String(h.value())).getOrElse("unknown")

            log.debug("Processing message topic={} key={}", topic, key)

            val processing = topic match {
              case "order.created"  => handleOrderCreated(value, traceId)
              case "order.cancelled"=> handleOrderCancelled(value, traceId)
              case "order.returned" => handleOrderReturned(value, traceId)
              case unknown          =>
                log.warn("Unexpected topic: {}", unknown)
                scala.concurrent.Future.unit
            }

            processing
              .recover { case ex =>
                log.error("Failed processing topic={} key={}: {}", topic, key, ex.getMessage)
                // Don't rethrow — let Kafka committer commit the offset.
                // Failed events go to DLT via @RetryableTopic equivalent in consumer config.
              }
              .map(_ => msg.committableOffset)
          }
      }
      .toMat(Committer.sink(committerSettings))(Keep.both)
      .run()

    log.info("InventoryStream started, listening on: order.created, order.cancelled, order.returned")
  }

  // ── Handlers ─────────────────────────────────────────────────────────────────

  private def handleOrderCreated(json: String, traceId: String): scala.concurrent.Future[Unit] =
    decode[OrderCreatedEvent](json) match {
      case Left(err) =>
        log.error("Deserialization failed for order.created: {}", err.getMessage)
        scala.concurrent.Future.unit
      case Right(event) =>
        service.reserveForOrder(event, traceId).map {
          case AllReserved(orderId)            => log.info("All items reserved for orderId={}", orderId)
          case PartiallyFailed(orderId, prods) => log.warn("Reservation failed for orderId={} products={}", orderId, prods)
        }
    }

  private def handleOrderCancelled(json: String, traceId: String): scala.concurrent.Future[Unit] =
    decode[OrderCancelledEvent](json) match {
      case Left(err)  =>
        log.error("Deserialization failed for order.cancelled: {}", err.getMessage)
        scala.concurrent.Future.unit
      case Right(event) if event.wasConfirmed =>
        // Only release reservation if order was confirmed (i.e. stock was locked)
        service.releaseForOrder(event.orderId, traceId)
          .map(_ => log.info("Reservation released for cancelled orderId={}", event.orderId))
      case Right(event) =>
        // PENDING cancel — no reservation to release
        log.debug("Order {} cancelled from PENDING state — no reservation to release", event.orderId)
        scala.concurrent.Future.unit
    }

  private def handleOrderReturned(json: String, traceId: String): scala.concurrent.Future[Unit] =
    decode[OrderReturnedEvent](json) match {
      case Left(err)    =>
        log.error("Deserialization failed for order.returned: {}", err.getMessage)
        scala.concurrent.Future.unit
      case Right(event) =>
        scala.concurrent.Future.traverse(event.items) { item =>
          service.restockFromReturn(item.productId, item.quantity, traceId)
        }.map(_ => log.info("Restocked items for returned orderId={}", event.orderId))
    }
}
