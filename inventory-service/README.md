# Inventory Service

Manages product stock levels using a **reactive Akka Streams pipeline** to consume Kafka events. Uses optimistic locking to safely handle concurrent reservations from multiple pods, and Redis as a cache-aside layer to reduce database load.

---

## Responsibilities

- Consume `order.created` → reserve stock for each order item
- Consume `order.cancelled` → release reservations (if order was confirmed)
- Consume `order.returned` → restock items
- Publish `inventory.updated` (RESERVED / RELEASED / RESTOCKED)
- Publish `order.cancel.requested` when stock is insufficient (saga compensation)
- REST API for stock queries and manual adjustments

---

## Reactive Pipeline Design

```
Kafka: order.created
        │
        ▼
CommittableSource (parallelism=4)
        │
        ▼
Deserialize OrderCreatedEvent
        │
        ▼
Per item: fetchFromCache (Redis) → if MISS: fetchFromDB → cacheResult
        │
        ▼
reserveWithOptimisticLock (retry 3×)
        │
        ├─ SUCCESS → DEL redis cache → publish inventory.updated (RESERVED)
        │
        └─ FAILURE → rollback all items → publish order.cancel.requested
        │
        ▼
commitOffset (Kafka offset committed only after success)
```

Backpressure is handled automatically by Akka Streams — consumers never overwhelm the database.

---

## Technology

| Component | Technology |
|-----------|-----------|
| Language | Scala 3.4.2 |
| Stream processing | Akka Streams 2.9.3 |
| Kafka consumer | Alpakka Kafka 5.0.0 |
| HTTP server | Akka HTTP 10.6.3 |
| Database | Slick 3.5.1 + HikariCP |
| Cache | Redis 7 via Lettuce (async) |
| DB migrations | Flyway 10.15.0 |
| JSON | Circe 0.14.9 |

---

## Running Locally

### With Docker Compose (recommended)

```bash
# From project root
docker-compose up inventory-service postgres redis kafka kafka-init
```

### Standalone

```bash
# From project root — requires Postgres, Redis, Kafka running
sbt "inventoryService/run"
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | `8082` | HTTP server port |
| `DB_URL` | `jdbc:postgresql://localhost:5432/ops_inventory` | PostgreSQL URL |
| `DB_USER` | `ops` | DB username |
| `DB_PASSWORD` | `changeme` | DB password |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |
| `REDIS_HOST` | `localhost` | Redis hostname |
| `REDIS_PORT` | `6379` | Redis port |

---

## API Endpoints

Base URL: `http://localhost:8082` (direct) or `http://localhost:8080/api/inventory` (via gateway)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/inventory?page=1&pageSize=20` | List all products |
| `GET` | `/inventory/{productId}` | Get stock level for a product |
| `PUT` | `/inventory/{productId}/stock` | Adjust stock (delta: positive=add, negative=remove) |
| `POST` | `/inventory/reserve` | Reserve stock directly (testing) |
| `GET` | `/health` | Liveness probe |

### Get Inventory — Example

```bash
curl http://localhost:8082/inventory/prod-001 \
  -H "X-Trace-Id: abc123"

# Response:
# {
#   "productId": "prod-001",
#   "sku": "WIDGET-S",
#   "availableQty": 48,
#   "reservedQty": 2,
#   "quantity": 50,
#   "cached": true
# }
```

---

## Kafka Topics

| Direction | Topic | Event |
|-----------|-------|-------|
| **Consumes** | `order.created` | Reserve stock for order items |
| **Consumes** | `order.cancelled` | Release reservation |
| **Consumes** | `order.returned` | Restock items |
| **Produces** | `inventory.updated` | RESERVED / RELEASED / RESTOCKED |
| **Produces** | `order.cancel.requested` | Insufficient stock (saga compensation) |

---

## Redis Cache

- **Key pattern**: `ops:inventory:{productId}`
- **TTL**: 300 seconds (5 minutes)
- **Eviction**: `allkeys-lru`
- **On write**: cache is invalidated immediately (`DEL`)
- **On Redis down**: fails open — reads fall back to Postgres transparently

---

## Database

Database: `ops_inventory`

| Table | Description |
|-------|-------------|
| `inventory` | Product stock with `version` column for optimistic locking |
| `inventory_reservations` | Per-order reservation tracking (ACTIVE → RELEASED / COMMITTED) |

Migrations: `src/main/resources/db/migration/`

---

## Optimistic Locking

Concurrent reservation from multiple pods uses atomic SQL:

```sql
UPDATE inventory
SET reserved_qty = reserved_qty + :qty,
    version      = version + 1
WHERE product_id = :id
  AND version    = :expectedVersion          -- lock check
  AND quantity - reserved_qty >= :qty        -- availability check
```

If `rowsUpdated = 0` → version conflict or insufficient stock → retry up to 3 times.

---

## Build

```bash
# From project root
sbt "inventoryService/assembly"       # Build fat JAR
sbt "inventoryService/test"           # Run unit tests
docker build -t ops/inventory-service ./inventory-service
```
