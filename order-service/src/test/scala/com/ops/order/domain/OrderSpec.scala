package com.ops.order.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

class OrderSpec extends AnyFlatSpec with Matchers {

  private val now     = Instant.now()
  private val address = ShippingAddress("123 Main St", "Springfield", "US", "12345")
  private val items   = List(ItemLine("prod-1", 2, BigDecimal("15.00")))

  private def orderWith(status: OrderStatus): Order = Order(
    id              = "ord-1",
    customerId      = "cust-1",
    items           = items,
    status          = status,
    totalAmount     = BigDecimal("30.00"),
    shippingAddress = address,
    idempotencyKey  = Some("idem-key-1"),
    createdAt       = now,
    updatedAt       = now
  )

  // ─── isTerminal ─────────────────────────────────────────────────────────────

  "Order.isTerminal" should "return true for Cancelled status" in {
    orderWith(OrderStatus.Cancelled).isTerminal shouldBe true
  }

  it should "return true for Returned status" in {
    orderWith(OrderStatus.Returned).isTerminal shouldBe true
  }

  it should "return false for Pending status" in {
    orderWith(OrderStatus.Pending).isTerminal shouldBe false
  }

  it should "return false for Confirmed status" in {
    orderWith(OrderStatus.Confirmed).isTerminal shouldBe false
  }

  it should "return false for Fulfilled status" in {
    orderWith(OrderStatus.Fulfilled).isTerminal shouldBe false
  }

  it should "return false for ReturnRequested status" in {
    orderWith(OrderStatus.ReturnRequested).isTerminal shouldBe false
  }

  // ─── canCancel ──────────────────────────────────────────────────────────────

  "Order.canCancel" should "return true for Pending status" in {
    orderWith(OrderStatus.Pending).canCancel shouldBe true
  }

  it should "return true for Confirmed status" in {
    orderWith(OrderStatus.Confirmed).canCancel shouldBe true
  }

  it should "return false for Fulfilled status" in {
    orderWith(OrderStatus.Fulfilled).canCancel shouldBe false
  }

  it should "return false for Cancelled status" in {
    orderWith(OrderStatus.Cancelled).canCancel shouldBe false
  }

  it should "return false for Returned status" in {
    orderWith(OrderStatus.Returned).canCancel shouldBe false
  }

  it should "return false for ReturnRequested status" in {
    orderWith(OrderStatus.ReturnRequested).canCancel shouldBe false
  }

  // ─── canRequestReturn ───────────────────────────────────────────────────────

  "Order.canRequestReturn" should "return true only for Fulfilled status" in {
    orderWith(OrderStatus.Fulfilled).canRequestReturn shouldBe true
  }

  it should "return false for Pending" in {
    orderWith(OrderStatus.Pending).canRequestReturn shouldBe false
  }

  it should "return false for Confirmed" in {
    orderWith(OrderStatus.Confirmed).canRequestReturn shouldBe false
  }

  it should "return false for Cancelled" in {
    orderWith(OrderStatus.Cancelled).canRequestReturn shouldBe false
  }

  it should "return false for ReturnRequested" in {
    orderWith(OrderStatus.ReturnRequested).canRequestReturn shouldBe false
  }

  it should "return false for Returned" in {
    orderWith(OrderStatus.Returned).canRequestReturn shouldBe false
  }

  // ─── canApproveReturn ───────────────────────────────────────────────────────

  "Order.canApproveReturn" should "return true only for ReturnRequested status" in {
    orderWith(OrderStatus.ReturnRequested).canApproveReturn shouldBe true
  }

  it should "return false for Fulfilled" in {
    orderWith(OrderStatus.Fulfilled).canApproveReturn shouldBe false
  }

  it should "return false for Confirmed" in {
    orderWith(OrderStatus.Confirmed).canApproveReturn shouldBe false
  }

  it should "return false for Returned" in {
    orderWith(OrderStatus.Returned).canApproveReturn shouldBe false
  }

  // ─── optional fields ────────────────────────────────────────────────────────

  "Order" should "have None for optional fields by default" in {
    val order = orderWith(OrderStatus.Pending)
    order.cancelledReason       shouldBe None
    order.cancelledAt           shouldBe None
    order.returnRequestedAt     shouldBe None
    order.returnReason          shouldBe None
    order.returnedAt            shouldBe None
    order.refundAmount          shouldBe None
  }

  it should "store cancellation fields when provided" in {
    val cancelledAt = Instant.now()
    val order = orderWith(OrderStatus.Cancelled).copy(
      cancelledReason = Some("CUSTOMER_REQUEST"),
      cancelledAt     = Some(cancelledAt)
    )
    order.cancelledReason shouldBe Some("CUSTOMER_REQUEST")
    order.cancelledAt     shouldBe Some(cancelledAt)
  }

  it should "store return fields when provided" in {
    val requestedAt = Instant.now()
    val order = orderWith(OrderStatus.ReturnRequested).copy(
      returnReason        = Some("Defective product"),
      returnRequestedAt   = Some(requestedAt),
      refundAmount        = Some(BigDecimal("30.00"))
    )
    order.returnReason      shouldBe Some("Defective product")
    order.returnRequestedAt shouldBe Some(requestedAt)
    order.refundAmount      shouldBe Some(BigDecimal("30.00"))
  }
}
