package com.ops.inventory.domain

import java.time.Instant

// ── Core domain object ────────────────────────────────────────────────────────
final case class InventoryItem(
  productId:   String,
  sku:         String,
  name:        String,
  quantity:    Int,
  reservedQty: Int,
  version:     Long,           // optimistic lock — must match DB on UPDATE
  updatedAt:   Instant
) {
  def availableQty: Int = quantity - reservedQty
  def canReserve(qty: Int): Boolean = availableQty >= qty
}

// ── Result of a reservation attempt ──────────────────────────────────────────
sealed trait ReservationResult
final case class Reserved(
  productId:    String,
  reservedQty:  Int,
  remainingQty: Int
) extends ReservationResult

final case class ReservationFailed(
  productId: String,
  reason:    String,           // INSUFFICIENT_STOCK | PRODUCT_NOT_FOUND | LOCK_CONFLICT
  available: Int
) extends ReservationResult
