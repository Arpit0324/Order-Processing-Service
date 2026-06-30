package com.ops.shared.events

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

class KafkaEventsSpec extends AnyFlatSpec with Matchers {

  private val now   = Instant.now()
  private val items = List(ItemLine("prod-1", 2, BigDecimal("15.00")))
  private val addr  = ShippingAddress("1 High St", "London", "GB", "EC1A 1BB")

  // ─── OrderCreatedEvent ───────────────────────────────────────────────────────

  "OrderCreatedEvent" should "have eventType ORDER_CREATED" in {
    val event = OrderCreatedEvent(
      eventId      = "ev-1",
      occurredAt   = now,
      traceId      = "t-1",
      orderId      = "ord-1",
      customerId   = "cust-1",
      items        = items,
      totalAmount  = BigDecimal("30.00"),
      shippingAddress = addr
    )
    event.eventType    shouldBe "ORDER_CREATED"
    event.eventVersion shouldBe 1
  }

  it should "implement KafkaEvent" in {
    val event: KafkaEvent = OrderCreatedEvent("ev-1", 1, now, "t-1", "ord-1", "cust-1",
      items, BigDecimal("30.00"), addr)
    event.traceId shouldBe "t-1"
  }

  // ─── OrderCancelledEvent ────────────────────────────────────────────────────

  "OrderCancelledEvent" should "have eventType ORDER_CANCELLED" in {
    val event = OrderCancelledEvent(
      eventId      = "ev-2",
      occurredAt   = now,
      traceId      = "t-2",
      orderId      = "ord-2",
      customerId   = "cust-2",
      reason       = "CUSTOMER_REQUEST",
      wasConfirmed = true,
      items        = items
    )
    event.eventType    shouldBe "ORDER_CANCELLED"
    event.wasConfirmed shouldBe true
    event.reason       shouldBe "CUSTOMER_REQUEST"
  }

  // ─── OrderCancelRequestedEvent ──────────────────────────────────────────────

  "OrderCancelRequestedEvent" should "have eventType ORDER_CANCEL_REQUESTED" in {
    val event = OrderCancelRequestedEvent(
      eventId        = "ev-3",
      occurredAt     = now,
      traceId        = "t-3",
      orderId        = "ord-3",
      reason         = "INSUFFICIENT_STOCK",
      failedProducts = List("prod-99")
    )
    event.eventType      shouldBe "ORDER_CANCEL_REQUESTED"
    event.failedProducts shouldBe List("prod-99")
  }

  // ─── OrderReturnedEvent ─────────────────────────────────────────────────────

  "OrderReturnedEvent" should "have eventType ORDER_RETURNED" in {
    val event = OrderReturnedEvent(
      eventId      = "ev-5",
      occurredAt   = now,
      traceId      = "t-5",
      orderId      = "ord-5",
      customerId   = "cust-5",
      items        = items,
      refundAmount = BigDecimal("15.00")
    )
    event.eventType    shouldBe "ORDER_RETURNED"
    event.refundAmount shouldBe BigDecimal("15.00")
  }

  // ─── InventoryUpdatedEvent ──────────────────────────────────────────────────

  "InventoryUpdatedEvent" should "have eventType INVENTORY_UPDATED" in {
    val event = InventoryUpdatedEvent(
      eventId      = "ev-6",
      occurredAt   = now,
      traceId      = "t-6",
      orderId      = "ord-6",
      productId    = "prod-1",
      reservedQty  = 10,
      remainingQty = 90,
      status       = "RESERVED"
    )
    event.eventType    shouldBe "INVENTORY_UPDATED"
    event.status       shouldBe "RESERVED"
    event.reservedQty  shouldBe 10
    event.remainingQty shouldBe 90
  }

  // ─── KafkaEvent trait ───────────────────────────────────────────────────────

  "All KafkaEvent subtypes" should "expose eventId, traceId and occurredAt" in {
    val events: Seq[KafkaEvent] = Seq(
      OrderCreatedEvent("e1", 1, now, "t", "o", "c", items, BigDecimal("1"), addr),
      OrderCancelledEvent("e2", 1, now, "t", "o", "c", "reason", false, items),
      OrderCancelRequestedEvent("e3", 1, now, "t", "o", "reason", List.empty)
    )
    events.foreach { e =>
      e.eventId      should not be empty
      e.traceId      should not be empty
      e.occurredAt   shouldBe now
      e.eventVersion shouldBe 1
    }
  }
}
