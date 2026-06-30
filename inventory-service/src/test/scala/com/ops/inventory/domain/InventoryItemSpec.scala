package com.ops.inventory.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import java.time.Instant

class InventoryItemSpec extends AnyFlatSpec with Matchers {

  private val now = Instant.now()

  private def item(quantity: Int, reservedQty: Int): InventoryItem =
    InventoryItem(
      productId   = "prod-1",
      sku         = "SKU-001",
      name        = "Test Widget",
      quantity    = quantity,
      reservedQty = reservedQty,
      version     = 1L,
      updatedAt   = now
    )

  // ─── availableQty ───────────────────────────────────────────────────────────

  "InventoryItem.availableQty" should "return quantity minus reservedQty" in {
    item(100, 30).availableQty shouldBe 70
  }

  it should "return 0 when all stock is reserved" in {
    item(50, 50).availableQty shouldBe 0
  }

  it should "return full quantity when nothing is reserved" in {
    item(100, 0).availableQty shouldBe 100
  }

  it should "return 0 when quantity is 0" in {
    item(0, 0).availableQty shouldBe 0
  }

  // ─── canReserve ─────────────────────────────────────────────────────────────

  "InventoryItem.canReserve" should "return true when enough stock is available" in {
    item(100, 20).canReserve(50) shouldBe true
  }

  it should "return true when reserving exactly the available quantity" in {
    item(100, 70).canReserve(30) shouldBe true
  }

  it should "return false when requested qty exceeds available qty" in {
    item(100, 80).canReserve(30) shouldBe false
  }

  it should "return false when no stock is available" in {
    item(100, 100).canReserve(1) shouldBe false
  }

  it should "return false when quantity is zero" in {
    item(0, 0).canReserve(1) shouldBe false
  }

  it should "return true when reserving 1 unit with 1 available" in {
    item(50, 49).canReserve(1) shouldBe true
  }

  // ─── ReservationResult ──────────────────────────────────────────────────────

  "Reserved" should "hold the correct reservation details" in {
    val result = Reserved("prod-1", 10, 40)
    result.productId    shouldBe "prod-1"
    result.reservedQty  shouldBe 10
    result.remainingQty shouldBe 40
  }

  "ReservationFailed" should "hold the reason and available qty" in {
    val result = ReservationFailed("prod-2", "INSUFFICIENT_STOCK", 5)
    result.productId shouldBe "prod-2"
    result.reason    shouldBe "INSUFFICIENT_STOCK"
    result.available shouldBe 5
  }

  "ReservationFailed" should "support PRODUCT_NOT_FOUND reason" in {
    val result = ReservationFailed("prod-3", "PRODUCT_NOT_FOUND", 0)
    result.reason shouldBe "PRODUCT_NOT_FOUND"
  }

  "ReservationFailed" should "support LOCK_CONFLICT reason" in {
    val result = ReservationFailed("prod-4", "LOCK_CONFLICT", 20)
    result.reason shouldBe "LOCK_CONFLICT"
  }

  "Reserved and ReservationFailed" should "both be subtypes of ReservationResult" in {
    val r1: ReservationResult = Reserved("p1", 5, 95)
    val r2: ReservationResult = ReservationFailed("p2", "INSUFFICIENT_STOCK", 0)
    r1 shouldBe a[Reserved]
    r2 shouldBe a[ReservationFailed]
  }
}
