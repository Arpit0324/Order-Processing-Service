package com.ops.order.domain

import com.ops.shared.domain.{ItemLine, ShippingAddress}
import java.time.Instant

// ── Order Status — 7-state machine (see plan LLD §2.11) ───────────────────────
sealed trait OrderStatus
object OrderStatus {
  case object Pending         extends OrderStatus
  case object Confirmed       extends OrderStatus
  case object Cancelled       extends OrderStatus
  case object Fulfilled       extends OrderStatus
  case object ReturnRequested extends OrderStatus
  case object Returned        extends OrderStatus

  def fromString(s: String): Either[String, OrderStatus] = s match {
    case "PENDING"          => Right(Pending)
    case "CONFIRMED"        => Right(Confirmed)
    case "CANCELLED"        => Right(Cancelled)
    case "FULFILLED"        => Right(Fulfilled)
    case "RETURN_REQUESTED" => Right(ReturnRequested)
    case "RETURNED"         => Right(Returned)
    case other              => Left(s"Unknown order status: $other")
  }

  def asString(s: OrderStatus): String = s match {
    case Pending         => "PENDING"
    case Confirmed       => "CONFIRMED"
    case Cancelled       => "CANCELLED"
    case Fulfilled       => "FULFILLED"
    case ReturnRequested => "RETURN_REQUESTED"
    case Returned        => "RETURNED"
  }
}
