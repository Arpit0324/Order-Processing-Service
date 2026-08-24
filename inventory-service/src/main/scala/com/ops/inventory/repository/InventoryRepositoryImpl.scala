package com.ops.inventory.repository

import com.ops.inventory.domain.InventoryItem
import slick.jdbc.PostgresProfile.api.*
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

// -- Slick table mapping
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

// -- Slick implementation
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

  // Fix A: UPDATE + INSERT in ONE db.run transactionally -- crash between them no longer possible
  override def reserveWithLock(productId: String, orderId: String,
                                qty: Int, version: Long): Future[Either[String, InventoryItem]] = {
    val now           = Instant.now().toEpochMilli
    val reservationId = java.util.UUID.randomUUID().toString

    val action = for {
      rowsUpdated <- sqlu"""
        UPDATE inventory
        SET reserved_qty = reserved_qty + $qty,
            version      = version + 1,
            updated_at   = $now
        WHERE product_id  = $productId
          AND version     = $version
          AND quantity - reserved_qty >= $qty
      """
      // ponytail: fail the transaction if UPDATE matched 0 rows (conflict or no stock)
      _ <- if (rowsUpdated == 0) DBIO.failed(new Exception("LOCK_CONFLICT_OR_INSUFFICIENT_STOCK"))
           else reservations += ReservationRow(reservationId, orderId, productId, qty, "ACTIVE", now)
      result <- inventory.filter(_.productId === productId).result.headOption
    } yield result

    db.run(action.transactionally).map {
      case Some(row) => Right(toDomain(row))
      case None      => Left("PRODUCT_NOT_FOUND")
    }.recover {
      case ex if ex.getMessage == "LOCK_CONFLICT_OR_INSUFFICIENT_STOCK" =>
        Left("LOCK_CONFLICT_OR_INSUFFICIENT_STOCK")
    }
  }

  override def releaseReservation(orderId: String): Future[Unit] = {
    val now = Instant.now().toEpochMilli
    // Fix G: release both ACTIVE and COMMITTED rows (crashed commits would ghost-lock stock)
    val action = for {
      rows <- reservations.filter(r => r.orderId === orderId &&
                (r.status === "ACTIVE" || r.status === "COMMITTED")).result
      _ <- DBIO.sequence(rows.map { row =>
        // Fix H: guard against underflow with GREATEST
        sqlu"""
          UPDATE inventory
          SET reserved_qty = GREATEST(reserved_qty - ${row.quantity}, 0),
              version      = version + 1,
              updated_at   = $now
          WHERE product_id = ${row.productId}
        """ >>
        reservations.filter(_.id === row.id).map(_.status).update("RELEASED")
      })
    } yield ()
    db.run(action.transactionally)
  }

  // Fix B: deduct quantity and reserved_qty when committing
  override def commitReservation(orderId: String): Future[Unit] = {
    val now = Instant.now().toEpochMilli
    val action = for {
      rows <- reservations.filter(r => r.orderId === orderId && r.status === "ACTIVE").result
      _ <- DBIO.sequence(rows.map { row =>
        sqlu"""
          UPDATE inventory
          SET quantity     = quantity - ${row.quantity},
              reserved_qty = GREATEST(reserved_qty - ${row.quantity}, 0),
              version      = version + 1,
              updated_at   = $now
          WHERE product_id = ${row.productId}
        """ >>
        reservations.filter(_.id === row.id).map(_.status).update("COMMITTED")
      })
    } yield ()
    db.run(action.transactionally)
  }

  override def restockFromReturn(productId: String, qty: Int): Future[Option[InventoryItem]] = {
    require(qty > 0, "restock qty must be positive") // Fix J
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

  // -- Mapping
  private def toRow(i: InventoryItem): InventoryRow =
    InventoryRow(i.productId, i.sku, i.name, i.quantity, i.reservedQty, i.version, i.updatedAt.toEpochMilli)

  private def toDomain(r: InventoryRow): InventoryItem =
    InventoryItem(r.productId, r.sku, r.name, r.quantity, r.reservedQty, r.version, Instant.ofEpochMilli(r.updatedAt))
}
