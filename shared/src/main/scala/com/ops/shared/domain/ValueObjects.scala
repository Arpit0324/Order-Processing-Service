package com.ops.shared.domain

import java.time.Instant

// ── Core value objects shared by all services ─────────────────────────────────

final case class Money(amount: BigDecimal, currency: String = "USD") {
  require(amount >= 0, s"Money amount cannot be negative: $amount")
  def +(other: Money): Money = {
    require(currency == other.currency, s"Cannot add different currencies: $currency vs ${other.currency}")
    copy(amount = amount + other.amount)
  }
}

final case class ItemLine(
  productId: String,
  quantity:  Int,
  unitPrice: BigDecimal
) {
  require(quantity > 0,   s"Quantity must be positive: $quantity")
  require(unitPrice >= 0, s"Unit price cannot be negative: $unitPrice")
  def subtotal: BigDecimal = unitPrice * quantity
}

final case class ShippingAddress(
  street:  String,
  city:    String,
  country: String,
  zip:     String
)
