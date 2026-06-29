package com.ops.inventory.api.dto

import com.ops.inventory.domain.InventoryItem
import java.time.Instant

// ── Input DTOs ────────────────────────────────────────────────────────────────
final case class ReserveRequest(orderId: String, productId: String, quantity: Int)
final case class UpdateStockRequest(delta: Int)

// ── Output DTOs ───────────────────────────────────────────────────────────────
final case class InventoryResponse(
  productId:    String,
  sku:          String,
  name:         String,
  quantity:     Int,
  reservedQty:  Int,
  availableQty: Int,
  version:      Long,
  lastUpdated:  Instant,
  cached:       Boolean = false
)

object InventoryResponse {
  def from(item: InventoryItem, cached: Boolean = false): InventoryResponse =
    InventoryResponse(
      productId    = item.productId,
      sku          = item.sku,
      name         = item.name,
      quantity     = item.quantity,
      reservedQty  = item.reservedQty,
      availableQty = item.availableQty,
      version      = item.version,
      lastUpdated  = item.updatedAt,
      cached       = cached
    )
}

final case class InventoryListResponse(items: List[InventoryResponse], total: Int, page: Int, pageSize: Int)
final case class ErrorResponse(code: String, message: String, traceId: String, timestamp: Instant = Instant.now())
