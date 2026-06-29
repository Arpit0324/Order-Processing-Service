package com.ops.order.domain

import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

// ── Core domain object — returned by service layer, stored in DB ──────────────
final case class Order(
  id:              String,
  customerId:      String,
  items:           List[ItemLine],
  status:          OrderStatus,
  totalAmount:     BigDecimal,
  shippingAddress: ShippingAddress,
  idempotencyKey:  Option[String],
  // Cancellation
  cancelledReason: Option[String]    = None,
  cancelledAt:     Option[Instant]   = None,
  // Return lifecycle
  returnRequestedAt: Option[Instant] = None,
  returnReason:    Option[String]    = None,
  returnedAt:      Option[Instant]   = None,
  refundAmount:    Option[BigDecimal] = None,
  createdAt:       Instant,
  updatedAt:       Instant
) {
  def isTerminal: Boolean =
    status == OrderStatus.Cancelled || status == OrderStatus.Returned

  def canCancel: Boolean =
    status == OrderStatus.Pending || status == OrderStatus.Confirmed

  def canRequestReturn: Boolean =
    status == OrderStatus.Fulfilled

  def canApproveReturn: Boolean =
    status == OrderStatus.ReturnRequested
}
