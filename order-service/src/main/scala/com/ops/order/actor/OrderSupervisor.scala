package com.ops.order.actor

import akka.actor.typed.{ActorRef, Behavior}
import akka.actor.typed.scaladsl.Behaviors
import com.ops.order.domain.OrderCommand

// ── OrderSupervisor — spawns and routes to per-order actors ──────────────────
// Receives any OrderCommand, extracts the orderId, and forwards to the
// child actor for that specific order (creating it if it doesn't exist yet).
//
// Akka Persistence ensures each actor has its own journal — no shared state.
object OrderSupervisor {

  sealed trait SupervisorCommand
  final case class ForwardToOrder(cmd: OrderCommand) extends SupervisorCommand

  def apply(): Behavior[ForwardToOrder] =
    Behaviors.setup { context =>
      Behaviors.receiveMessage { case ForwardToOrder(cmd) =>
        val orderId = extractOrderId(cmd)
        val actorRef = context.child(orderId) match {
          case Some(ref) => ref.asInstanceOf[ActorRef[OrderCommand]]
          case None      =>
            context.log.info("Spawning new OrderActor for orderId={}", orderId)
            context.spawn(OrderActor(orderId), orderId)
        }
        actorRef ! cmd
        Behaviors.same
      }
    }

  private def extractOrderId(cmd: OrderCommand): String = cmd match {
    case c: com.ops.order.domain.CreateOrder    => c.orderId
    case c: com.ops.order.domain.ConfirmOrder   => c.orderId
    case c: com.ops.order.domain.CancelOrder    => c.orderId
    case c: com.ops.order.domain.FulfillOrder   => c.orderId
    case c: com.ops.order.domain.RequestReturn  => c.orderId
    case c: com.ops.order.domain.ApproveReturn  => c.orderId
    case c: com.ops.order.domain.RejectReturn   => c.orderId
    case c: com.ops.order.domain.GetOrder       => c.orderId
    case c: com.ops.order.domain.SlaTimeout     => c.orderId
  }
}
