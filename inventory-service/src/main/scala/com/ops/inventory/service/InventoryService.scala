package com.ops.inventory.service

import com.ops.inventory.api.dto.{InventoryResponse, ReserveRequest, UpdateStockRequest}
import com.ops.shared.events.OrderCreatedEvent
import scala.concurrent.Future

trait InventoryService {
  def getItem(productId: String):                          Future[Option[InventoryResponse]]
  def listItems(page: Int, pageSize: Int):                 Future[(List[InventoryResponse], Int)]
  def reserveForOrder(event: OrderCreatedEvent,
                      traceId: String):                    Future[ReservationOutcome]
  def releaseForOrder(orderId: String,
                      traceId: String):                    Future[Unit]
  def commitForOrder(orderId: String):                     Future[Unit]
  def restockFromReturn(productId: String, qty: Int,
                        traceId: String):                  Future[Option[InventoryResponse]]
  def updateStock(productId: String, delta: Int,
                  traceId: String):                        Future[Option[InventoryResponse]]
}

// Outcome of processing a single order.created event
sealed trait ReservationOutcome
final case class AllReserved(orderId: String)             extends ReservationOutcome
final case class PartiallyFailed(orderId: String,
  failedProducts: List[String])                           extends ReservationOutcome
