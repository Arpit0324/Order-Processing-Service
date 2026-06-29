package com.ops.order.domain

import com.ops.shared.domain.{ItemLine, ShippingAddress}
import akka.actor.typed.ActorRef
import java.time.Instant

// ── Commands — sent TO the OrderActor ────────────────────────────────────────
// Every command carries a replyTo so the caller gets a typed response.
sealed trait OrderCommand

final case class CreateOrder(
  orderId:         String,
  customerId:      String,
  items:           List[ItemLine],
  shippingAddress: ShippingAddress,
  totalAmount:     BigDecimal,
  idempotencyKey:  Option[String],
  replyTo:         ActorRef[OrderReply]
) extends OrderCommand

final case class ConfirmOrder(
  orderId: String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class CancelOrder(
  orderId: String,
  reason:  String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class FulfillOrder(
  orderId: String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class RequestReturn(
  orderId: String,
  reason:  String,
  items:   List[ItemLine],
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class ApproveReturn(
  orderId: String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class RejectReturn(
  orderId: String,
  reason:  String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

final case class GetOrder(
  orderId: String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand

// Internal: fired by Akka timer when order stays PENDING too long
private[order] final case class SlaTimeout(
  orderId: String,
  replyTo: ActorRef[OrderReply]
) extends OrderCommand
