package com.ops.order.api.dto

import com.ops.order.domain.{Order, OrderStatus}
import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

// ── Input DTOs (endpoint layer → service layer) ───────────────────────────────

final case class CreateOrderRequest(
  customerId:      String,
  items:           List[ItemLine],
  shippingAddress: ShippingAddress,
  idempotencyKey:  Option[String] = None
)

final case class CancelOrderRequest(reason: String)

final case class RequestReturnRequest(reason: String)

final case class RejectReturnRequest(reason: String)

// ── Output DTOs (service layer → endpoint layer → client) ────────────────────

final case class OrderResponse(
  orderId:     String,
  customerId:  String,
  status:      String,
  items:       List[ItemLine],
  totalAmount: BigDecimal,
  createdAt:   Instant,
  updatedAt:   Instant,
  // optional fields — only present when relevant
  cancelledReason:    Option[String]    = None,
  cancelledAt:        Option[Instant]   = None,
  returnRequestedAt:  Option[Instant]   = None,
  returnedAt:         Option[Instant]   = None,
  refundAmount:       Option[BigDecimal] = None
)

object OrderResponse {
  def from(o: Order): OrderResponse = OrderResponse(
    orderId            = o.id,
    customerId         = o.customerId,
    status             = OrderStatus.asString(o.status),
    items              = o.items,
    totalAmount        = o.totalAmount,
    createdAt          = o.createdAt,
    updatedAt          = o.updatedAt,
    cancelledReason    = o.cancelledReason,
    cancelledAt        = o.cancelledAt,
    returnRequestedAt  = o.returnRequestedAt,
    returnedAt         = o.returnedAt,
    refundAmount       = o.refundAmount
  )
}

final case class OrderListResponse(
  orders:   List[OrderResponse],
  total:    Int,
  page:     Int,
  pageSize: Int
)

// ── Error response (uniform across all endpoints) ────────────────────────────
final case class ErrorResponse(
  code:      String,
  message:   String,
  traceId:   String,
  timestamp: Instant = Instant.now()
)
