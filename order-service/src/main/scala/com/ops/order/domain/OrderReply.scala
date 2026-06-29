package com.ops.order.domain

// ── Replies — typed responses sent back to callers of OrderActor ─────────────
sealed trait OrderReply

final case class OrderCreatedReply(order: Order)  extends OrderReply
final case class OrderUpdatedReply(order: Order)  extends OrderReply
final case class OrderDetailsReply(order: Order)  extends OrderReply
final case class OrderNotFound(orderId: String)   extends OrderReply
final case class OrderRejected(reason: String)    extends OrderReply
