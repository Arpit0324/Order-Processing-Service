package com.ops.shared.domain

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ValueObjectsSpec extends AnyFlatSpec with Matchers {

  // ─── Money ──────────────────────────────────────────────────────────────────

  "Money" should "default currency to USD" in {
    Money(BigDecimal("10.00")).currency shouldBe "USD"
  }

  it should "allow zero amount" in {
    Money(BigDecimal("0")).amount shouldBe BigDecimal("0")
  }

  it should "allow positive amount" in {
    Money(BigDecimal("99.99")).amount shouldBe BigDecimal("99.99")
  }

  it should "throw IllegalArgumentException for negative amount" in {
    an[IllegalArgumentException] should be thrownBy Money(BigDecimal("-0.01"))
  }

  it should "add two Money values with the same currency" in {
    val total = Money(BigDecimal("10.00")) + Money(BigDecimal("5.50"))
    total.amount   shouldBe BigDecimal("15.50")
    total.currency shouldBe "USD"
  }

  it should "throw when adding Money with different currencies" in {
    an[IllegalArgumentException] should be thrownBy {
      Money(BigDecimal("10.00"), "USD") + Money(BigDecimal("5.00"), "EUR")
    }
  }

  it should "support non-USD currencies" in {
    Money(BigDecimal("20.00"), "EUR").currency shouldBe "EUR"
  }

  it should "be additive and commutative for same currency" in {
    val a = Money(BigDecimal("3.00"))
    val b = Money(BigDecimal("7.00"))
    (a + b).amount shouldBe (b + a).amount
  }

  // ─── ItemLine ────────────────────────────────────────────────────────────────

  "ItemLine" should "compute subtotal as unitPrice * quantity" in {
    ItemLine("prod-1", 3, BigDecimal("10.00")).subtotal shouldBe BigDecimal("30.00")
  }

  it should "compute subtotal correctly for fractional prices" in {
    ItemLine("prod-2", 2, BigDecimal("9.99")).subtotal shouldBe BigDecimal("19.98")
  }

  it should "allow zero unit price" in {
    ItemLine("prod-free", 5, BigDecimal("0")).subtotal shouldBe BigDecimal("0")
  }

  it should "throw IllegalArgumentException for zero quantity" in {
    an[IllegalArgumentException] should be thrownBy ItemLine("prod-1", 0, BigDecimal("10.00"))
  }

  it should "throw IllegalArgumentException for negative quantity" in {
    an[IllegalArgumentException] should be thrownBy ItemLine("prod-1", -1, BigDecimal("10.00"))
  }

  it should "throw IllegalArgumentException for negative unit price" in {
    an[IllegalArgumentException] should be thrownBy ItemLine("prod-1", 1, BigDecimal("-0.01"))
  }

  it should "hold the correct productId" in {
    ItemLine("prod-xyz", 1, BigDecimal("1.00")).productId shouldBe "prod-xyz"
  }

  // ─── ShippingAddress ─────────────────────────────────────────────────────────

  "ShippingAddress" should "store all fields correctly" in {
    val addr = ShippingAddress("10 Downing St", "London", "GB", "SW1A 2AA")
    addr.street  shouldBe "10 Downing St"
    addr.city    shouldBe "London"
    addr.country shouldBe "GB"
    addr.zip     shouldBe "SW1A 2AA"
  }

  it should "support equality for same address" in {
    val addr1 = ShippingAddress("123 Main St", "Springfield", "US", "12345")
    val addr2 = ShippingAddress("123 Main St", "Springfield", "US", "12345")
    addr1 shouldBe addr2
  }

  it should "not be equal to different addresses" in {
    val addr1 = ShippingAddress("123 Main St", "Springfield", "US", "12345")
    val addr2 = ShippingAddress("456 Elm St",  "Shelbyville", "US", "67890")
    addr1 should not equal addr2
  }

  it should "allow copy to change individual fields" in {
    val original = ShippingAddress("123 Main St", "Springfield", "US", "12345")
    val moved    = original.copy(city = "Shelbyville")
    moved.city    shouldBe "Shelbyville"
    moved.street  shouldBe original.street
    moved.country shouldBe original.country
    moved.zip     shouldBe original.zip
  }
}
