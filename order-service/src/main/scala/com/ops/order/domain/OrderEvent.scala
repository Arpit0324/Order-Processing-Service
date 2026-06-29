package com.ops.order.domain

import com.ops.shared.domain.ItemLine
import java.time.Instant

// ── Events — what gets PERSISTED to the Akka journal ─────────────────────────
// Events are immutable facts. They are the source of truth.
// The actor state is fully reconstructed by replaying these events on restart.
sealed trait OrderEvent

final case class OrderCreated(
  orderId:         String,
  customerId:      String,
  items:           List[ItemLine],
  shippingAddress: com.ops.shared.domain.ShippingAddress,
  totalAmount:     BigDecimal,
  idempotencyKey:  Option[String],
  at:              Instant
) extends OrderEvent

final case class OrderConfirmed(
  orderId: String,
  at:      Instant
) extends OrderEvent

final case class OrderCancelled(
  orderId:      String,
  reason:       String,
  wasConfirmed: Boolean,
  at:           Instant
) extends OrderEvent

final case class OrderFulfilled(
  orderId: String,
  at:      Instant
) extends OrderEvent

final case class ReturnRequested(
  orderId: String,
  reason:  String,
  items:   List[ItemLine],
  at:      Instant
) extends OrderEvent

final case class ReturnApproved(
  orderId:      String,
  refundAmount: BigDecimal,
  at:           Instant
) extends OrderEvent

final case class ReturnRejected(
  orderId: String,
  reason:  String,
  at:      Instant
) extends OrderEvent
