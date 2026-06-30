package com.ops.order.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

class OrderStatusSpec extends AnyFlatSpec with Matchers {

  "OrderStatus.fromString" should "return Pending for 'PENDING'" in {
    OrderStatus.fromString("PENDING") shouldBe Right(OrderStatus.Pending)
  }

  it should "return Confirmed for 'CONFIRMED'" in {
    OrderStatus.fromString("CONFIRMED") shouldBe Right(OrderStatus.Confirmed)
  }

  it should "return Cancelled for 'CANCELLED'" in {
    OrderStatus.fromString("CANCELLED") shouldBe Right(OrderStatus.Cancelled)
  }

  it should "return Fulfilled for 'FULFILLED'" in {
    OrderStatus.fromString("FULFILLED") shouldBe Right(OrderStatus.Fulfilled)
  }

  it should "return ReturnRequested for 'RETURN_REQUESTED'" in {
    OrderStatus.fromString("RETURN_REQUESTED") shouldBe Right(OrderStatus.ReturnRequested)
  }

  it should "return Returned for 'RETURNED'" in {
    OrderStatus.fromString("RETURNED") shouldBe Right(OrderStatus.Returned)
  }

  it should "return Left for unknown status" in {
    OrderStatus.fromString("INVALID") shouldBe a[Left[?, ?]]
    OrderStatus.fromString("INVALID").left.get should include("Unknown order status")
  }

  it should "return Left for empty string" in {
    OrderStatus.fromString("") shouldBe a[Left[?, ?]]
  }

  "OrderStatus.asString" should "return 'PENDING' for Pending" in {
    OrderStatus.asString(OrderStatus.Pending) shouldBe "PENDING"
  }

  it should "return 'CONFIRMED' for Confirmed" in {
    OrderStatus.asString(OrderStatus.Confirmed) shouldBe "CONFIRMED"
  }

  it should "return 'CANCELLED' for Cancelled" in {
    OrderStatus.asString(OrderStatus.Cancelled) shouldBe "CANCELLED"
  }

  it should "return 'FULFILLED' for Fulfilled" in {
    OrderStatus.asString(OrderStatus.Fulfilled) shouldBe "FULFILLED"
  }

  it should "return 'RETURN_REQUESTED' for ReturnRequested" in {
    OrderStatus.asString(OrderStatus.ReturnRequested) shouldBe "RETURN_REQUESTED"
  }

  it should "return 'RETURNED' for Returned" in {
    OrderStatus.asString(OrderStatus.Returned) shouldBe "RETURNED"
  }

  "OrderStatus round-trip" should "preserve all statuses via fromString/asString" in {
    val statuses = Seq(
      OrderStatus.Pending, OrderStatus.Confirmed, OrderStatus.Cancelled,
      OrderStatus.Fulfilled, OrderStatus.ReturnRequested, OrderStatus.Returned
    )
    for (status <- statuses) {
      OrderStatus.fromString(OrderStatus.asString(status)) shouldBe Right(status)
    }
  }
}
