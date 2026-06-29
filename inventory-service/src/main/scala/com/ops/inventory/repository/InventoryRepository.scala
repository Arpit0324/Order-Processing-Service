package com.ops.inventory.repository

import com.ops.inventory.domain.{InventoryItem, ReservationResult}
import scala.concurrent.Future

// ── Repository trait ──────────────────────────────────────────────────────────
trait InventoryRepository {
  def findById(productId: String):                          Future[Option[InventoryItem]]
  def findAll(page: Int, pageSize: Int):                    Future[(List[InventoryItem], Int)]

  // Atomic: UPDATE WHERE version = $v AND available >= qty; returns Right on success, Left on conflict/insufficient
  def reserveWithLock(productId: String, orderId: String,
                      qty: Int, version: Long):             Future[Either[String, InventoryItem]]

  // Release reservation on order cancel/return
  def releaseReservation(orderId: String):                  Future[Unit]

  // Commit reservation to permanent deduction on order fulfill
  def commitReservation(orderId: String):                   Future[Unit]

  // Restock on return approved
  def restockFromReturn(productId: String, qty: Int):       Future[Option[InventoryItem]]

  def updateQuantity(productId: String, delta: Int):        Future[Option[InventoryItem]]
  def save(item: InventoryItem):                            Future[InventoryItem]
}
