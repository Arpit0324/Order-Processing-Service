package com.ops.order.service

import com.ops.order.api.dto.{CreateOrderRequest, OrderListResponse, OrderResponse}
import scala.concurrent.Future

// ── Service layer contract — endpoint and Kafka consumer call this ────────────
trait OrderService {
  def createOrder(req: CreateOrderRequest, traceId: String): Future[Either[ServiceError, OrderResponse]]
  def getOrder(id: String):                                  Future[Option[OrderResponse]]
  def listOrders(customerId: String, page: Int, pageSize: Int): Future[OrderListResponse]
  def cancelOrder(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]]
  def fulfillOrder(id: String, traceId: String):             Future[Either[ServiceError, OrderResponse]]
  def requestReturn(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]]
  def approveReturn(id: String, traceId: String):            Future[Either[ServiceError, OrderResponse]]
  def rejectReturn(id: String, reason: String, traceId: String): Future[Either[ServiceError, OrderResponse]]
}

// ── Typed service errors — mapped to HTTP status by endpoint layer ────────────
sealed trait ServiceError
final case class NotFound(id: String)             extends ServiceError
final case class InvalidTransition(reason: String) extends ServiceError
final case class DuplicateRequest(orderId: String) extends ServiceError
final case class ValidationError(msg: String)     extends ServiceError
final case class InternalError(msg: String)       extends ServiceError
