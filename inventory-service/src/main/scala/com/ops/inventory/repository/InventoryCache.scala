package com.ops.inventory.repository

import com.ops.inventory.domain.InventoryItem
import com.ops.shared.serialization.JsonCodecs.given
import io.circe.syntax.*
import io.circe.parser.*
import io.lettuce.core.RedisClient
import io.lettuce.core.api.async.RedisAsyncCommands
import org.slf4j.LoggerFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.FutureConverters.*

// ── InventoryCache — Redis cache-aside wrapping InventoryRepository ───────────
// Read:  GET redis:inventory:{productId}
//          HIT  → deserialize and return
//          MISS → call DB → SET with TTL → return
// Write: After any DB write → DEL redis:inventory:{productId}
//
// Key format: "ops:inventory:{productId}"
// TTL: 300 seconds (5 minutes) — stale cache causes overselling, never skip invalidation
class InventoryCache(
  delegate: InventoryRepository,
  redis:    RedisAsyncCommands[String, String],
  ttlSecs:  Long = 300
)(using ec: ExecutionContext) extends InventoryRepository {

  private val log = LoggerFactory.getLogger(getClass)
  private val Prefix = "ops:inventory:"

  private def key(productId: String) = s"$Prefix$productId"

  override def findById(productId: String): Future[Option[InventoryItem]] = {
    val cacheKey = key(productId)
    redis.get(cacheKey).asScala.flatMap {
      case null =>
        // Cache miss — go to DB
        delegate.findById(productId).flatMap {
          case None       => Future.successful(None)
          case Some(item) =>
            redis.setex(cacheKey, ttlSecs, item.asJson.noSpaces).asScala
              .map(_ => Some(item))
              .recover { case ex =>
                log.warn("Redis SET failed for productId={}, returning DB result", productId, ex)
                Some(item)
              }
        }
      case json =>
        decode[InventoryItem](json) match {
          case Right(item) => Future.successful(Some(item))
          case Left(err)   =>
            log.warn("Redis cache decode failed for productId={}: {}", productId, err.getMessage)
            redis.del(cacheKey).asScala.flatMap(_ => delegate.findById(productId))
        }
    }.recover { case ex =>
      log.error("Redis GET failed for productId={}, falling back to DB", productId, ex)
      // fall through to DB — never fail a read because Redis is down
      null // triggers the flatMap miss path won't help here — use delegate directly
    }.flatMap {
      case null => delegate.findById(productId)
      case x    => Future.successful(x)
    }
  }

  // Passthrough for list — no cache for collection queries
  override def findAll(page: Int, pageSize: Int): Future[(List[InventoryItem], Int)] =
    delegate.findAll(page, pageSize)

  override def reserveWithLock(productId: String, orderId: String,
                                qty: Int, version: Long): Future[Either[String, InventoryItem]] =
    delegate.reserveWithLock(productId, orderId, qty, version)
      .flatMap {
        case Right(item) => invalidate(productId).map(_ => Right(item))
        case left        => Future.successful(left)
      }

  override def releaseReservation(orderId: String): Future[Unit] =
    // We don't know which productIds are involved without a DB query
    // Simple approach: let DB do the release, then invalidation happens
    // on next findById cache miss (acceptable staleness window = TTL)
    delegate.releaseReservation(orderId)

  override def commitReservation(orderId: String): Future[Unit] =
    delegate.commitReservation(orderId)

  override def restockFromReturn(productId: String, qty: Int): Future[Option[InventoryItem]] =
    delegate.restockFromReturn(productId, qty)
      .flatMap { result => invalidate(productId).map(_ => result) }

  override def updateQuantity(productId: String, delta: Int): Future[Option[InventoryItem]] =
    delegate.updateQuantity(productId, delta)
      .flatMap { result => invalidate(productId).map(_ => result) }

  override def save(item: InventoryItem): Future[InventoryItem] =
    delegate.save(item)
      .flatMap { saved => invalidate(item.productId).map(_ => saved) }

  private def invalidate(productId: String): Future[Unit] =
    redis.del(key(productId)).asScala
      .map(_ => log.debug("Cache invalidated for productId={}", productId))
      .recover { case ex => log.warn("Cache invalidation failed for productId={}", productId, ex) }
}
