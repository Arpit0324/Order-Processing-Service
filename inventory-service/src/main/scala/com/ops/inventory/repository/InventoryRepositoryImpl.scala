package com.ops.inventory.repository

import com.ops.inventory.domain.InventoryItem
import slick.jdbc.PostgresProfile.api.*
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

// ── Slick table mapping ───────────────────────────────────────────────────────
class InventoryTable(tag: Tag) extends Table[InventoryRow](tag, "inventory") {
  def productId   = column[String]("product_id", O.PrimaryKey)
  def sku         = column[String]("sku")
  def name        = column[String]("name")
  def quantity    = column[Int]("quantity")
  def reservedQty = column[Int]("reserved_qty")
  def version     = column[Long]("version")
  def updatedAt   = column[Long]("updated_at")
  def * = (productId, sku, name, quantity, reservedQty, version, updatedAt).mapTo[InventoryRow]
}

class ReservationsTable(tag: Tag) extends Table[ReservationRow](tag, "inventory_reservations") {
  def id         = column[String]("id", O.PrimaryKey)
  def orderId    = column[String]("order_id")
  def productId  = column[String]("product_id")
  def quantity   = column[Int]("quantity")
  def status     = column[String]("status")
  def createdAt  = column[Long]("created_at")
  def * = (id, orderId, productId, quantity, status, createdAt).mapTo[ReservationRow]
}

final case class InventoryRow(productId: String, sku: String, name: String,
                               quantity: Int, reservedQty: Int, version: Long, updatedAt: Long)
final case class ReservationRow(id: String, orderId: String, productId: String,
                                 quantity: Int, status: String, createdAt: Long)

// ── Slick implementation ──────────────────────────────────────────────────────
class InventoryRepositoryImpl(db: Database)(using ec: ExecutionContext) extends InventoryRepository {

  private val inventory    = TableQuery[InventoryTable]
  private val reservations = TableQuery[ReservationsTable]

  override def findById(productId: String): Future[Option[InventoryItem]] =
    db.run(inventory.filter(_.productId === productId).result.headOption).map(_.map(toDomain))

  override def findAll(page: Int, pageSize: Int): Future[(List[InventoryItem], Int)] = {
    val base  = inventory.sortBy(_.sku)
    val paged = base.drop((page - 1) * pageSize).take(pageSize).result
    val count = base.length.result
    db.run(paged zip count).map { case (rows, total) => (rows.map(toDomain).toList, total) }
  }

  // Atomic reserve with optimistic locking:
  // UPDATE inventory SET reserved_qty = reserved_qty + qty, version = version + 1
  // WHERE product_id = $id AND version = $v AND quantity - reserved_qty >= qty
  override def reserveWithLock(productId: String, orderId: String,
                                qty: Int, version: Long): Future[Either[String, InventoryItem]] = {
    val now = Instant.now().toEpochMilli
    val reservationId = java.util.UUID.randomUUID().toString

    val action = for {
      updated <- inventory
        .filter(r => r.productId === productId &&
                     r.version === version &&
                     (r.quantity - r.reservedQty) >= qty)
        .map(r => (r.reservedQty, r.version, r.updatedAt))
        .update((0, version + 1, now)) // placeholder — see note below

      // Note: Slick doesn't support computed updates directly. Use sqlu for atomic increment.
      _ <- sqlu"""
        UPDATE inventory
        SET reserved_qty = reserved_qty + $qty,
            version      = version + 1,
            updated_at   = $now
        WHERE product_id  = $productId
          AND version     = $version
          AND quantity - reserved_qty >= $qty
      """

      _ <- (reservations += ReservationRow(
        id        = reservationId,
        orderId   = orderId,
        productId = productId,
        quantity  = qty,
        status    = "ACTIVE",
        createdAt = now
      ))

      result <- inventory.filter(_.productId === productId).result.headOption
    } yield result

    // Use raw SQL for the atomic update to avoid Slick computed column limitation
    val rawAction = DBIO.seq(
      sqlu"""
        UPDATE inventory
        SET reserved_qty = reserved_qty + $qty,
            version      = version + 1,
            updated_at   = $now
        WHERE product_id  = $productId
          AND version     = $version
          AND quantity - reserved_qty >= $qty
      """,
      reservations += ReservationRow(reservationId, orderId, productId, qty, "ACTIVE", now)
    ).transactionally

    db.run(
      sqlu"""
        UPDATE inventory
        SET reserved_qty = reserved_qty + $qty,
            version      = version + 1,
            updated_at   = $now
        WHERE product_id  = $productId
          AND version     = $version
          AND quantity - reserved_qty >= $qty
      """.transactionally
    ).flatMap { rowsUpdated =>
      if (rowsUpdated == 0)
        Future.successful(Left("LOCK_CONFLICT_OR_INSUFFICIENT_STOCK"))
      else
        db.run((reservations += ReservationRow(reservationId, orderId, productId, qty, "ACTIVE", now)).transactionally)
          .flatMap(_ => findById(productId))
          .map {
            case Some(item) => Right(item)
            case None       => Left("PRODUCT_NOT_FOUND")
          }
    }
  }

  override def releaseReservation(orderId: String): Future[Unit] = {
    val now = Instant.now().toEpochMilli
    val action = for {
      rows <- reservations.filter(r => r.orderId === orderId && r.status === "ACTIVE").result
      _ <- DBIO.sequence(rows.map { row =>
        sqlu"""
          UPDATE inventory
          SET reserved_qty = reserved_qty - ${row.quantity},
              version      = version + 1,
              updated_at   = $now
          WHERE product_id = ${row.productId}
        """ >>
        reservations.filter(_.id === row.id).map(_.status).update("RELEASED")
      })
    } yield ()
    db.run(action.transactionally)
  }

  override def commitReservation(orderId: String): Future[Unit] = {
    val action = reservations
      .filter(r => r.orderId === orderId && r.status === "ACTIVE")
      .map(_.status).update("COMMITTED")
    db.run(action.transactionally).map(_ => ())
  }

  override def restockFromReturn(productId: String, qty: Int): Future[Option[InventoryItem]] = {
    val now = Instant.now().toEpochMilli
    db.run(
      sqlu"""
        UPDATE inventory
        SET quantity   = quantity + $qty,
            version    = version + 1,
            updated_at = $now
        WHERE product_id = $productId
      """.transactionally
    ).flatMap(_ => findById(productId))
  }

  override def updateQuantity(productId: String, delta: Int): Future[Option[InventoryItem]] = {
    val now = Instant.now().toEpochMilli
    db.run(
      sqlu"""
        UPDATE inventory
        SET quantity   = quantity + $delta,
            version    = version + 1,
            updated_at = $now
        WHERE product_id = $productId
      """.transactionally
    ).flatMap(_ => findById(productId))
  }

  override def save(item: InventoryItem): Future[InventoryItem] =
    db.run((inventory += toRow(item)).transactionally).map(_ => item)

  // ── Mapping ─────────────────────────────────────────────────────────────────
  private def toRow(i: InventoryItem): InventoryRow =
    InventoryRow(i.productId, i.sku, i.name, i.quantity, i.reservedQty, i.version, i.updatedAt.toEpochMilli)

  private def toDomain(r: InventoryRow): InventoryItem =
    InventoryItem(r.productId, r.sku, r.name, r.quantity, r.reservedQty, r.version, Instant.ofEpochMilli(r.updatedAt))
}
