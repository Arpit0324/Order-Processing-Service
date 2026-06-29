package com.ops.order.repository

import com.ops.order.domain.{Order, OrderStatus}
import com.ops.shared.domain.{ItemLine, ShippingAddress}
import com.ops.order.infra.DatabaseConfig
import slick.jdbc.PostgresProfile.api.*
import io.circe.syntax.*
import io.circe.parser.*
import com.ops.shared.serialization.JsonCodecs.given
import java.time.Instant
import scala.concurrent.{ExecutionContext, Future}

// ── Slick table mapping ───────────────────────────────────────────────────────
class OrdersTable(tag: Tag) extends Table[OrderRow](tag, "orders") {
  def id                = column[String]("id", O.PrimaryKey)
  def customerId        = column[String]("customer_id")
  def status            = column[String]("status")
  def items             = column[String]("items")           // JSONB stored as TEXT
  def totalAmount       = column[BigDecimal]("total_amount")
  def shippingAddress   = column[String]("shipping_address") // JSONB stored as TEXT
  def idempotencyKey    = column[Option[String]]("idempotency_key")
  def cancelledReason   = column[Option[String]]("cancelled_reason")
  def cancelledAt       = column[Option[Long]]("cancelled_at")
  def returnRequestedAt = column[Option[Long]]("return_requested_at")
  def returnReason      = column[Option[String]]("return_reason")
  def returnedAt        = column[Option[Long]]("returned_at")
  def refundAmount      = column[Option[BigDecimal]]("refund_amount")
  def createdAt         = column[Long]("created_at")
  def updatedAt         = column[Long]("updated_at")

  def * = (id, customerId, status, items, totalAmount, shippingAddress,
           idempotencyKey, cancelledReason, cancelledAt,
           returnRequestedAt, returnReason, returnedAt, refundAmount,
           createdAt, updatedAt).mapTo[OrderRow]
}

// Flat DB row — separate from domain Order to keep mapping explicit
final case class OrderRow(
  id:                String,
  customerId:        String,
  status:            String,
  items:             String,
  totalAmount:       BigDecimal,
  shippingAddress:   String,
  idempotencyKey:    Option[String],
  cancelledReason:   Option[String],
  cancelledAt:       Option[Long],
  returnRequestedAt: Option[Long],
  returnReason:      Option[String],
  returnedAt:        Option[Long],
  refundAmount:      Option[BigDecimal],
  createdAt:         Long,
  updatedAt:         Long
)

// ── Slick implementation ──────────────────────────────────────────────────────
class OrderRepositoryImpl(db: Database)(using ec: ExecutionContext) extends OrderRepository {

  private val orders = TableQuery[OrdersTable]

  override def save(order: Order): Future[Order] = {
    val row = toRow(order)
    val action = (orders += row).map(_ => order)
    db.run(action)
  }

  override def findById(id: String): Future[Option[Order]] =
    db.run(orders.filter(_.id === id).result.headOption).map(_.map(toDomain))

  override def findByCustomer(customerId: String, page: Int, pageSize: Int): Future[(List[Order], Int)] = {
    val base   = orders.filter(_.customerId === customerId).sortBy(_.createdAt.desc)
    val paged  = base.drop((page - 1) * pageSize).take(pageSize).result
    val count  = base.length.result
    db.run(paged zip count).map { case (rows, total) => (rows.map(toDomain).toList, total) }
  }

  override def updateStatus(id: String, status: OrderStatus,
                             extraFields: Map[String, Any]): Future[Option[Order]] = {
    val now = Instant.now().toEpochMilli
    val q = orders.filter(_.id === id)
      .map(o => (o.status, o.updatedAt))
      .update((OrderStatus.asString(status), now))
    db.run(q).flatMap(_ => findById(id))
  }

  override def findStaleByStatus(status: OrderStatus, olderThanMinutes: Int): Future[List[Order]] = {
    val cutoff = Instant.now().minusSeconds(olderThanMinutes * 60L).toEpochMilli
    db.run(
      orders
        .filter(o => o.status === OrderStatus.asString(status) && o.createdAt < cutoff)
        .result
    ).map(_.map(toDomain).toList)
  }

  // ── Mapping helpers ─────────────────────────────────────────────────────────

  private def toRow(o: Order): OrderRow = OrderRow(
    id                = o.id,
    customerId        = o.customerId,
    status            = OrderStatus.asString(o.status),
    items             = o.items.asJson.noSpaces,
    totalAmount       = o.totalAmount,
    shippingAddress   = o.shippingAddress.asJson.noSpaces,
    idempotencyKey    = o.idempotencyKey,
    cancelledReason   = o.cancelledReason,
    cancelledAt       = o.cancelledAt.map(_.toEpochMilli),
    returnRequestedAt = o.returnRequestedAt.map(_.toEpochMilli),
    returnReason      = o.returnReason,
    returnedAt        = o.returnedAt.map(_.toEpochMilli),
    refundAmount      = o.refundAmount,
    createdAt         = o.createdAt.toEpochMilli,
    updatedAt         = o.updatedAt.toEpochMilli
  )

  private def toDomain(r: OrderRow): Order = {
    val items   = decode[List[ItemLine]](r.items).getOrElse(Nil)
    val address = decode[ShippingAddress](r.shippingAddress).getOrElse(
      ShippingAddress("", "", "", ""))
    Order(
      id                = r.id,
      customerId        = r.customerId,
      status            = OrderStatus.fromString(r.status).getOrElse(OrderStatus.Pending),
      items             = items,
      totalAmount       = r.totalAmount,
      shippingAddress   = address,
      idempotencyKey    = r.idempotencyKey,
      cancelledReason   = r.cancelledReason,
      cancelledAt       = r.cancelledAt.map(Instant.ofEpochMilli),
      returnRequestedAt = r.returnRequestedAt.map(Instant.ofEpochMilli),
      returnReason      = r.returnReason,
      returnedAt        = r.returnedAt.map(Instant.ofEpochMilli),
      refundAmount      = r.refundAmount,
      createdAt         = Instant.ofEpochMilli(r.createdAt),
      updatedAt         = Instant.ofEpochMilli(r.updatedAt)
    )
  }
}
