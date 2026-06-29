package com.ops.order.repository

import com.ops.order.domain.{Order, OrderStatus}
import scala.concurrent.Future

// ── Repository trait — service layer depends on this, not the impl ───────────
// Keeps Slick/DB details out of business logic.
trait OrderRepository {
  def save(order: Order):                                  Future[Order]
  def findById(id: String):                               Future[Option[Order]]
  def findByCustomer(customerId: String,
                     page: Int, pageSize: Int):           Future[(List[Order], Int)]
  def updateStatus(id: String, status: OrderStatus,
                   extraFields: Map[String, Any] = Map.empty): Future[Option[Order]]
  def findStaleByStatus(status: OrderStatus,
                        olderThanMinutes: Int):           Future[List[Order]]
}
