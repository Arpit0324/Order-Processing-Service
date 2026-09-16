package com.ops.shared.events

import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

// ── Base envelope — every Kafka message is wrapped in this ───────────────────
// eventVersion enables schema evolution without breaking consumers
sealed trait KafkaEvent {
  def eventId:      String
  def eventType:    String
  def eventVersion: Int
  def occurredAt:   Instant
  def traceId:      String   // OpenTelemetry trace ID for cross-service correlation
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: order.created  (key = orderId)
// Producer: order-service   Consumers: inventory-service, notification-service
// Schema version: 1 (add fields only; bump eventVersion for breaking changes)
// ─────────────────────────────────────────────────────────────────────────────
final case class OrderCreatedEvent(
  eventId:         String,
  eventVersion:    Int     = 1,
  occurredAt:      Instant,
  traceId:         String,
  orderId:         String,
  customerId:      String,
  items:           List[ItemLine],
  totalAmount:     BigDecimal,
  shippingAddress: ShippingAddress,
  customerEmail:   Option[String] = None,   // recipient snapshot for notification-service
  customerPhone:   Option[String] = None
) extends KafkaEvent {
  val eventType = "ORDER_CREATED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: order.cancelled  (key = orderId)
// wasConfirmed=true means inventory was reserved and must be released
// Producer: order-service   Consumers: inventory-service, notification-service
// Schema version: 1
// ─────────────────────────────────────────────────────────────────────────────
final case class OrderCancelledEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  orderId:      String,
  customerId:   String,
  reason:       String,         // CUSTOMER_REQUEST | INSUFFICIENT_STOCK | SLA_TIMEOUT | PAYMENT_FAILED
  wasConfirmed: Boolean,        // if true, inventory service must release reservation
  items:        List[ItemLine], // needed by inventory to know which products to release
  customerEmail: Option[String] = None,
  customerPhone: Option[String] = None
) extends KafkaEvent {
  val eventType = "ORDER_CANCELLED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: order.cancel.requested  (key = orderId)
// Saga compensation — published by Inventory Service on reservation failure
// Producer: inventory-service   Consumers: order-service, notification-service
// Schema version: 1
// ─────────────────────────────────────────────────────────────────────────────
final case class OrderCancelRequestedEvent(
  eventId:        String,
  eventVersion:   Int     = 1,
  occurredAt:     Instant,
  traceId:        String,
  orderId:        String,
  reason:         String,       // INSUFFICIENT_STOCK
  failedProducts: List[String], // productIds that had insufficient stock
  customerEmail:  Option[String] = None,
  customerPhone:  Option[String] = None
) extends KafkaEvent {
  val eventType = "ORDER_CANCEL_REQUESTED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: order.return.requested  (key = orderId)
// ─────────────────────────────────────────────────────────────────────────────
final case class OrderReturnRequestedEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  orderId:      String,
  customerId:   String,
  reason:       String,
  items:        List[ItemLine]
) extends KafkaEvent {
  val eventType = "ORDER_RETURN_REQUESTED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: order.returned  (key = orderId)
// Producer: order-service   Consumers: notification-service
// Schema version: 1
// ─────────────────────────────────────────────────────────────────────────────
final case class OrderReturnedEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  orderId:      String,
  customerId:   String,
  items:        List[ItemLine],
  refundAmount: BigDecimal,
  customerEmail: Option[String] = None,
  customerPhone: Option[String] = None
) extends KafkaEvent {
  val eventType = "ORDER_RETURNED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: inventory.updated  (key = productId)
// ─────────────────────────────────────────────────────────────────────────────
final case class InventoryUpdatedEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  orderId:      String,
  productId:    String,
  reservedQty:  Int,
  remainingQty: Int,
  status:       String  // RESERVED | REJECTED | RELEASED | RESTOCKED
) extends KafkaEvent {
  val eventType = "INVENTORY_UPDATED"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: notif.sent  (key = notifId)
// Producer: notification-service   Consumers: (audit / analytics — none yet)
// Schema version: 1 — contract fixture: notification-service contracts/notif.sent.v1.json
// ─────────────────────────────────────────────────────────────────────────────
final case class NotificationSentEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  notifId:      String,
  orderId:      String,
  channel:      String, // EMAIL | SMS | PUSH
  recipient:    String,
  template:     String,
  status:       String  // DELIVERED | FAILED
) extends KafkaEvent {
  val eventType = "NOTIFICATION_SENT"
}

// ─────────────────────────────────────────────────────────────────────────────
// Topic: refund.requested  (key = orderId)
// ─────────────────────────────────────────────────────────────────────────────
final case class RefundRequestedEvent(
  eventId:      String,
  eventVersion: Int     = 1,
  occurredAt:   Instant,
  traceId:      String,
  orderId:      String,
  customerId:   String,
  refundAmount: BigDecimal,
  currency:     String = "USD"
) extends KafkaEvent {
  val eventType = "REFUND_REQUESTED"
}
