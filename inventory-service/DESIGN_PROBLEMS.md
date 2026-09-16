# Inventory Service — Design Problems & Gaps

## 🔴 Critical Bugs (Data Integrity)

### A. Split-Transaction Reserve (Data Loss on Crash)
`reserveWithLock` does **two separate `db.run()` calls** — one to UPDATE `inventory`, one to INSERT into `inventory_reservations`. A crash between them permanently locks stock with no matching reservation row; it will never be released.
```scala
// ponytail: these must be ONE db.run(action.transactionally)
db.run(sqlu"UPDATE inventory ...".transactionally)
  .flatMap(_ => db.run((reservations += ...).transactionally))
```
**Fix:** Combine both into a single `DBIO.seq(...).transactionally` in one `db.run`.

---

### B. `commitReservation` Never Deducts Quantity
Committing an order only flips `status = 'COMMITTED'`. It never does:
- `quantity = quantity - committed_qty`
- `reserved_qty = reserved_qty - committed_qty`

After every fulfilled order, `inventory.quantity` reflects pre-sale numbers. The table is permanently wrong after any fulfilled order.

---

### C. `POST /inventory/reserve` Is Read-Only
The HTTP reserve endpoint calls `service.getItem` (a pure read) and returns 200 OK. It **never calls `service.reserveForOrder`**. Two concurrent requests on 1 unit both succeed. This is a silent correctness bug disguised as a working endpoint.

---

### D. No Idempotency on `order.created` Kafka Events
If offset commit fails after processing, the same event replays. No unique constraint on `inventory_reservations(order_id, product_id)` → **double reservation / double stock deduction**.

**Fix:** Add `UNIQUE(order_id, product_id)` constraint on `inventory_reservations`.

---

### E. `releaseReservation` Leaves Cache Stale
Cache invalidation is skipped on `releaseReservation` and `commitReservation`. Released stock stays invisible for up to 300s, blocking valid orders.

---

## 🔴 Security

| Issue | Severity |
|---|---|
| Default DB password `"changeme"` committed in `application.conf` | Critical |
| No auth on HTTP endpoints — anyone can call `PUT /inventory/{id}/stock` | Critical |
| `X-Trace-Id` echoed unsanitized into logs — log injection possible | Medium |
| Redis has no AUTH or TLS (`redis://host:port`) | Medium |
| Kafka has no SSL/SASL config | Medium |
| Full product catalog + quantities exposed without auth via `GET /inventory` | Low |

---

## 🟠 Serious Design Flaws

### F. Parallel Saga Rollback Race
`Future.traverse` reserves multiple items concurrently. If item A succeeds and item B fails, rollback calls `releaseForOrder` — but item A's DB write may not have committed yet. Partial reservations can leak.

### G. `releaseForOrder` Only Releases `ACTIVE` Rows
If a crash leaves some rows as `COMMITTED`, `releaseForOrder` skips them. Those rows permanently ghost-lock stock.

### H. `reserved_qty` Can Underflow on Double-Release
```sql
UPDATE inventory SET reserved_qty = reserved_qty - qty
```
No `CHECK (reserved_qty - qty >= 0)`. Event replay causes negative `reserved_qty`, DB CHECK constraint throws an unhandled 500.

### I. Error Swallowing in Kafka Stream
```scala
processing.recover { case ex => log.error("...") }
  .map(_ => msg.committableOffset)  // offset committed on failure!
```
Any processing error is silently committed. Events are permanently lost — no DLQ, no retry, no alert. Comment mentions "DLT equivalent" but it's not implemented.

### J. `restockFromReturn` Accepts Negative Quantity
`qty = -50` silently reduces stock. No `require(qty > 0)` guard.

### K. `publishAllReserved` Emits `remainingQty = 0` Always
Hardcoded `0` instead of actual remaining stock. Any downstream consumer relying on this for low-stock alerts will always see 0.

### L. `publishReleased` Has Empty `productId`
```scala
InventoryUpdatedEvent(..., productId = "", ...)
```
Downstream consumers cannot determine which products changed.

---

## 🟡 Architecture / Separation of Concerns

| Problem | Location |
|---|---|
| Business logic (`availableQty >= qty` check) in controller | `InventoryController.scala:70–75` |
| `InventoryRepositoryImpl` manages two tables with no separation | Should split `ReservationRepository` |
| `InventoryApp` is a God-object that wires everything | Hard to test, hard to extend |
| `null` sentinel used in Scala cache fallback instead of `Option` | `InventoryCache.scala` |
| `cached: Boolean` on `InventoryResponse` never set to `true` | Dead field |
| Hard-coded topic names as string literals across files | Any rename breaks silently |

---

## 🟡 Missing Input Validation

| Missing | Impact |
|---|---|
| `page = -1` or `pageSize = 100000` accepted by `listItems` | Negative OFFSET or huge DB scan |
| `productId` not validated as UUID | Malformed ID causes JDBC exception, not clean 400 |
| `UpdateStockRequest.delta` has no min/max bounds | `Int.MaxValue` overflows `quantity + delta` in Postgres |
| `updateQuantity` SQL has no `CHECK (quantity + delta >= 0)` | Negative stock possible via bad delta |

---

## 🟡 Missing Indexes

| Missing Index | Query Affected |
|---|---|
| `inventory_reservations(order_id, status)` | `releaseReservation` filters on both; current index only covers `order_id` |
| `inventory(product_id, version)` | Optimistic lock UPDATE; version filter is unindexed |

---

## 🟡 N+1 / Performance

- `releaseReservation` runs **N sequential SQL round-trips** for N reservation rows instead of a single bulk `UPDATE ... WHERE id IN (...)`.
- `GET /inventory` with no `pageSize` cap + `sortBy(_.sku)` with no index = full table scan + sort on large datasets.
- Slick `numThreads = 20` + HikariCP `maximumPoolSize = 20` are separate pools, causing contention.
- All DB futures run on the Pekko dispatcher — JDBC calls starve the actor system. Needs a dedicated `ExecutionContext`.

---

## 🟠 Kafka / Messaging Gaps

| Gap | Detail |
|---|---|
| No `acks = "all"`, `retries`, or `enable.idempotence` on producer | Messages silently lost on broker leader re-election |
| Hard-coded consumer group `"inventory-service"` | Staging/prod sharing Kafka cluster consume each other's events |
| No schema registry / Avro | Plain JSON strings — no schema versioning or backwards-compatibility |
| One Kafka event per item, not per order | Consumers must correlate by `orderId`; no atomic "all reserved" event |

---

## 🔴 Missing Production Features

| Feature | Why It Matters |
|---|---|
| **Reservation TTL / expiry** | Reservations never expire. A stuck order permanently locks stock forever |
| **Dead-letter queue (DLQ)** | Failed Kafka events are silently dropped; no recovery path |
| **Deep health check** | `GET /health` returns static `{"status":"UP"}` — doesn't check DB, Redis, or Kafka |
| **Low-stock alerts** | No threshold-based alerting when stock drops below a configurable level |
| **Stock adjustment reason codes** | `updateStock` takes only `delta` — no reason (damaged / returned / manual) |
| **Admin CRUD API** | No HTTP endpoint to create or delete inventory items |
| **Pessimistic locking fallback** | Optimistic lock fails under high contention with no `SELECT FOR UPDATE` fallback |
| **Batch reserve API** | No single-call atomic reserve for multiple products |
| **Audit log** | No record of who changed what stock, when, and why |
| **Metrics / observability** | No Prometheus metrics: no reservation rate, cache hit ratio, DB query latency |
| **Cursor-based pagination** | Offset pagination is slow on large datasets |

---

## 🟡 Config / Infrastructure Gaps

| Gap | Detail |
|---|---|
| `baselineOnMigrate = true` in Flyway | Can silently ignore existing DB state in production |
| No HikariCP `connectionTestQuery` | Stale connections not detected |
| No Redis AUTH or TLS | Open Redis connection |
| No JVM tuning | No GC flags, heap sizing, or thread pool config |
| No graceful drain on shutdown | In-flight `mapAsync` Kafka futures abandoned on kill signal |
| Redis TTL 300s hardcoded | Stale cache blocks valid orders for 5 minutes after stock change |
| `pekko.actor.provider = "local"` | Undocumented intentional constraint — breaks silently if clustering is ever added |

---

## 🟡 Test Coverage

Only `InventoryItemSpec` exists (pure domain unit tests).

**Missing:**
- Repository tests (Testcontainers + real Postgres)
- Service tests (mock repository)
- HTTP route tests (Pekko-HTTP `testkit`)
- Kafka consumer integration tests
- Concurrent reservation stress test (the race condition in point A would be caught here)

---

## Priority Order

1. **Fix split-transaction reserve** (A) — data integrity
2. **Fix `commitReservation`** (B) — stock accounting is broken
3. **Fix HTTP reserve endpoint** (C) — the whole feature is a no-op
4. **Add Kafka idempotency constraint** (D) — double-reservation
5. **Add auth on HTTP endpoints** — security
6. **Add reservation TTL** — stuck orders leak stock forever
7. **Add DLQ for Kafka failures** — events are silently lost
8. **Fix cache invalidation on release/commit** (E)
