# Order Processing System — System Design & Implementation Plan

> **Document version**: 2.0  
> **Last updated**: 2026-06-29  
> **Status**: Ready for implementation

---

## Table of Contents

1. [HLD — High Level Design](#hld)
   - 1.1 System Context
   - 1.2 Component Architecture
   - 1.3 Technology Rationale (ADRs)
   - 1.4 Non-Functional Requirements
   - 1.5 Data Flow — Happy Path
   - 1.6 Data Flow — Saga Compensation
   - 1.7 Deployment Topology
2. [LLD — Low Level Design](#lld)
   - 2.1 API Contracts
   - 2.2 Kafka Message Schemas
   - 2.3 Database ERD & Schemas
   - 2.3a Database Schema Migrations (Flyway)
   - 2.4 Order Service — Class Design & State Machine
   - 2.5 Inventory Service — Stream Pipeline Design
   - 2.6 Notification Service — Actor Design
   - 2.7 API Gateway — Filter Chain
   - 2.8 Caching Strategy
   - 2.9 Error Handling & Resilience
   - 2.10 Layered Architecture: Endpoint → Service → Repository
   - 2.11 Order Lifecycle: Pending / Confirmed / Cancelled / Returned
3. [Implementation Plan](#impl)
   - Phase 0–7
4. [Improvements & Suggestions](#improvements)
5. [Verification Checklist](#verify)

---

# 1. HLD — High Level Design <a name="hld"></a>

## 1.1 System Context

```
┌──────────────────────────────────────────────────────────────────────┐
│                         Kubernetes Cluster                           │
│                                                                      │
│   External Traffic                                                   │
│   ───────────────                                                    │
│   Client / Browser ──► nginx Ingress ──► API Gateway :8080          │
│                                          (Spring Boot + Swagger)     │
│                                               │                      │
│   ┌───────────────────────────────────────────┤                      │
│   │               Internal ClusterIP          │                      │
│   ▼               ────────────────            ▼                      │
│  Order Service :8081      Inventory Service :8082                    │
│  (Scala + Akka Actors)    (Scala + Akka Streams)                     │
│         │                        │                                   │
│         │                 Notification Service :8083                 │
│         │                 (Java + Spring + Akka)                     │
│         │                        │                                   │
│         └────────────┬───────────┘                                   │
│                      ▼                                               │
│             ┌────────────────┐                                       │
│             │  Kafka :9092   │  Topics:                              │
│             │  (KRaft mode)  │  • order.created      (3 part)        │
│             └───────┬────────┘  • inventory.updated  (3 part)        │
│                     │           • notif.sent          (1 part)        │
│          ┌──────────┴──────────┐• *.DLT              (1 part each)   │
│          ▼                     ▼                                     │
│     PostgreSQL :5432       Redis :6379                               │
│     (Primary store)        (Cache + Idempotency keys)                │
│                                                                      │
│  Observability Stack:                                                │
│  Prometheus → Grafana │ Jaeger (OTel traces) │ Kafka UI :8090        │
└──────────────────────────────────────────────────────────────────────┘
```

## 1.2 Component Architecture

| Component | Technology | Role | Scaling |
|-----------|-----------|------|---------|
| API Gateway | Spring Boot 3 + Spring Cloud Gateway (reactive) | Auth, routing, rate-limiting, circuit breaker, Swagger UI | HPA CPU 70%, 2–6 replicas |
| Order Service | Scala 3 + Akka Typed + Akka Persistence | Order lifecycle management, event sourcing, Kafka producer | HPA CPU 70%, 2–10 replicas |
| Inventory Service | Scala 3 + Akka Streams + Alpakka Kafka | Stock reservation, reactive Kafka consumer pipeline | KEDA Kafka lag, 2–12 replicas |
| Notification Service | Java 21 + Spring Boot 3 + Akka Typed | Multi-channel notification dispatch, Kafka consumer | HPA CPU 50%, 2–4 replicas |
| Shared Library | Scala 3 (JAR + cross-compiled stubs) | Kafka event schemas, JSON codecs, domain value objects | N/A |
| PostgreSQL 16 | Managed DB (or StatefulSet) | Durable order + inventory + notification records | Read replica for reporting |
| Redis 7 | In-memory cache | Inventory cache, idempotency keys, rate-limit counters | Cluster mode for HA |
| Kafka 7 (KRaft) | Event streaming | Async event bus, event replay, DLT | 3-broker cluster in prod |

## 1.3 Technology Rationale — Architecture Decision Records

### ADR-001: Akka Actors for Order Service (not Spring reactive)
- **Decision**: Use Akka Typed Persistent Actors
- **Reason**: Order lifecycle is a classic state machine with audit requirements. Akka Persistence gives event sourcing (full audit trail) with zero extra infrastructure. Each order is a persistent actor — commands are validated against current state before persisting events.
- **Rejected**: Spring WebFlux + RDBMS state — no native event sourcing; Kafka Streams state store — overkill for per-entity state.

### ADR-002: Akka Streams for Inventory Service (not Actors)
- **Decision**: Use Akka Streams reactive pipeline
- **Reason**: Inventory updates are a continuous, high-throughput stream of events. Akka Streams provides built-in backpressure, parallelism control, and graph DSL. No per-entity state needed — pure transformation pipeline.
- **Rejected**: Akka Actors — fine-grained actor-per-product has high overhead at scale.

### ADR-003: Choreography Saga (not Orchestration)
- **Decision**: Saga via Kafka events only — no central orchestrator
- **Reason**: Avoids single point of failure. Services publish/consume compensating events. Gateway does not know about saga steps — it's fire-and-forget.
- **Trade-off**: Harder to visualize saga state. Mitigated by OTel tracing.

### ADR-004: KRaft Kafka (no Zookeeper)
- **Decision**: Use Kafka 3.6+ KRaft mode
- **Reason**: Eliminates Zookeeper operational overhead. Simpler Docker Compose and K8s manifests. GA since Kafka 3.3.

### ADR-005: Redis for cache + idempotency (not Hazelcast)
- **Decision**: Redis 7 with allkeys-lru eviction
- **Reason**: Already needed for rate limiting at gateway. Using it also for inventory cache and idempotency keys avoids adding a second cache tier.

## 1.4 Non-Functional Requirements

| NFR | Target | Mechanism |
|-----|--------|-----------|
| Order creation latency (P99) | < 200ms | Async Kafka publish; actor responds before Kafka ack |
| Inventory reservation latency (P99) | < 500ms (end-to-end) | Redis cache-aside; Kafka consumer lag < 1000 |
| System availability | 99.9% (3 nines) | K8s HPA + rolling deploys + circuit breakers |
| Throughput | 500 orders/sec peak | 3 Kafka partitions × 4 parallel stream workers = 12 concurrent reservations |
| Data durability | No order lost | Kafka RF=2 + Postgres WAL + Akka Persistence journal |
| Security | OWASP Top 10 compliant | JWT RS256, parameterized queries, input validation, secrets manager |
| Observability | Full trace per request | OTel → Jaeger; Prometheus metrics; structured JSON logs |

## 1.5 Data Flow — Happy Path (Order Creation)

```
┌────────┐  POST /api/orders      ┌─────────┐  HTTP POST /orders   ┌───────────────┐
│ Client │ ─────────────────────► │ Gateway │ ──────────────────►  │ Order Service │
└────────┘  {items, customerId,   └─────────┘  (JWT stripped,       └───────┬───────┘
            idempotencyKey}        validates JWT, rate-limit check)          │
                                                                     persist OrderCreated
                                                                     to Akka Journal (Postgres)
                                                                             │
                                                                    publish to Kafka
                                                                    "order.created"
                                                                             │
                                            ┌────────────────────────────────┤
                                            ▼                                ▼
                                  ┌──────────────────┐            ┌───────────────────┐
                                  │ Inventory Service│            │Notification Service│
                                  │ Akka Streams     │            │ @KafkaListener     │
                                  └────────┬─────────┘            └────────┬──────────┘
                                  check Redis cache                dispatch to
                                  → reserve in Postgres            NotificationActor
                                  → invalidate cache               → send email/SMS
                                  → publish "inventory.updated"    → publish "notif.sent"
                                           │
                                  Order Service consumes
                                  "inventory.updated"
                                  → actor receives ConfirmOrder cmd
                                  → status: PENDING → CONFIRMED
```

## 1.6 Data Flow — Saga Compensation (Inventory Failure)

```
Order Service          Kafka                Inventory Service       Order Service
     │                   │                        │                      │
     │── OrderCreated ──►│── order.created ───────►│                      │
     │                   │                  [stock check FAILS]            │
     │                   │                        │                        │
     │                   │◄── order.cancel.req ───│                        │
     │                   │    (compensation evt)  │                        │
     │◄── consume ────────│                        │                        │
     │  [CancelOrder cmd] │                        │                        │
     │  PENDING→CANCELLED │                        │                        │
     │                   │                        │                        │
     │── OrderCancelled ──►│── order.cancelled ──────────────────────────►│
                          │                              NotificationService│
                          │                              sends "out of stock"│
                          │                              notification         │
```

## 1.7 Deployment Topology

```
                        Internet
                           │
                    ┌──────▼──────┐
                    │ nginx Ingress│  (K8s Ingress resource)
                    │  :443/80    │  TLS termination
                    └──────┬──────┘
                           │
              ┌────────────▼────────────┐
              │     api-gateway         │  LoadBalancer Service
              │  Deployment: 2-6 pods   │  :8080
              │  HPA: CPU 70%           │
              └────────────┬────────────┘
                           │  ClusterIP
         ┌─────────────────┼─────────────────┐
         ▼                 ▼                 ▼
   order-service    inventory-service  notification-svc
   Deployment       Deployment         Deployment
   2-10 pods        2-12 pods          2-4 pods
   HPA: CPU 70%     KEDA: Kafka lag    HPA: CPU 50%
         │                 │                 │
         └────────┬────────┘                 │
                  ▼                          │
           StatefulSet                       │
         kafka (3 brokers)◄──────────────────┘
                  │
    ┌─────────────┼─────────────┐
    ▼             ▼             ▼
 postgres      redis        kafka-ui
 StatefulSet   Deployment   Deployment
 (primary +    (cluster)    :8090
  1 replica)
```

---

# 2. LLD — Low Level Design <a name="lld"></a>

## 2.1 API Contracts

### POST /api/orders — Create Order

**Request**
```json
{
  "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
  "items": [
    { "productId": "prod-001", "quantity": 2, "unitPrice": 29.99 },
    { "productId": "prod-002", "quantity": 1, "unitPrice": 149.00 }
  ],
  "shippingAddress": {
    "street": "123 Main St", "city": "Springfield", "country": "US", "zip": "62701"
  }
}
```

**Headers**: `Authorization: Bearer <JWT>`, `Idempotency-Key: <UUIDv4>`

**Response 201**
```json
{
  "orderId": "ord-7f3a...",
  "status": "PENDING",
  "totalAmount": 208.98,
  "createdAt": "2026-06-29T10:30:00Z",
  "_links": { "self": "/api/orders/ord-7f3a..." }
}
```

**Error responses**

| HTTP | Code | Condition |
|------|------|-----------|
| 400 | INVALID_REQUEST | Missing fields or constraint violation |
| 401 | UNAUTHORIZED | Missing/expired JWT |
| 409 | DUPLICATE_ORDER | Same `Idempotency-Key` already processed |
| 422 | INSUFFICIENT_STOCK | Inventory reservation failed |
| 503 | SERVICE_UNAVAILABLE | Circuit breaker open |

---

### GET /api/orders/{id}

**Response 200**
```json
{
  "orderId": "ord-7f3a...",
  "customerId": "3fa85f64...",
  "status": "CONFIRMED",
  "items": [ ... ],
  "totalAmount": 208.98,
  "events": [
    { "type": "ORDER_CREATED",   "at": "2026-06-29T10:30:00Z" },
    { "type": "STOCK_RESERVED",  "at": "2026-06-29T10:30:02Z" },
    { "type": "ORDER_CONFIRMED", "at": "2026-06-29T10:30:02Z" }
  ]
}
```

---

### GET /api/inventory/{productId}

**Response 200**
```json
{
  "productId": "prod-001",
  "availableQty": 48,
  "reservedQty": 2,
  "lastUpdated": "2026-06-29T10:30:02Z",
  "cached": true
}
```

---

### POST /api/inventory/reserve

**Request**
```json
{
  "orderId": "ord-7f3a...",
  "reservations": [
    { "productId": "prod-001", "quantity": 2 }
  ]
}
```

**Response 200 / 422 (insufficient stock)**

---

## 2.2 Kafka Message Schemas

### Topic: `order.created`

```json
{
  "eventId":    "evt-uuid-v4",
  "eventType":  "ORDER_CREATED",
  "eventVersion": 1,
  "occurredAt": "2026-06-29T10:30:00Z",
  "traceId":    "otel-trace-id",
  "payload": {
    "orderId":    "ord-7f3a...",
    "customerId": "3fa85f64...",
    "items": [
      { "productId": "prod-001", "quantity": 2, "unitPrice": 29.99 }
    ],
    "totalAmount": 208.98,
    "shippingAddress": { ... }
  }
}
```

**Key**: `orderId` (ensures all events for an order go to same partition)

### Topic: `inventory.updated`

```json
{
  "eventId":    "evt-uuid-v4",
  "eventType":  "INVENTORY_UPDATED",
  "eventVersion": 1,
  "occurredAt": "2026-06-29T10:30:02Z",
  "traceId":    "otel-trace-id",
  "payload": {
    "orderId":     "ord-7f3a...",
    "productId":   "prod-001",
    "reservedQty": 2,
    "remainingQty": 48,
    "status":      "RESERVED"   // RESERVED | REJECTED | RELEASED
  }
}
```

**Key**: `productId`

### Topic: `notif.sent`

```json
{
  "eventId":   "evt-uuid-v4",
  "eventType": "NOTIFICATION_SENT",
  "occurredAt": "2026-06-29T10:30:03Z",
  "payload": {
    "notifId":   "notif-uuid",
    "orderId":   "ord-7f3a...",
    "channel":   "EMAIL",          // EMAIL | SMS | PUSH
    "recipient": "user@example.com",
    "template":  "ORDER_CONFIRMED",
    "status":    "DELIVERED"       // DELIVERED | FAILED
  }
}
```

### Topic: `order.cancel.requested` (Saga compensation)

```json
{
  "eventType": "ORDER_CANCEL_REQUESTED",
  "payload": {
    "orderId": "ord-7f3a...",
    "reason":  "INSUFFICIENT_STOCK",
    "failedProducts": ["prod-001"]
  }
}
```

---

## 2.3 Database ERD & Schemas

### Entity-Relationship Diagram

```
┌─────────────────────┐         ┌───────────────────────┐
│       orders        │         │     order_events       │
├─────────────────────┤         ├───────────────────────┤
│ id UUID PK          │ 1─────► │ ordering UUID PK       │
│ customer_id UUID    │         │ persistence_id TEXT FK │
│ status VARCHAR(20)  │         │ sequence_nr BIGINT     │
│ items JSONB         │         │ event_type TEXT        │
│ total NUMERIC(12,2) │         │ payload JSONB          │
│ idempotency_key TEXT│         │ created_at TIMESTAMPTZ │
│ created_at TSTZ     │         └───────────────────────┘
│ updated_at TSTZ     │
└─────────────────────┘

┌───────────────────────────────┐
│          inventory            │
├───────────────────────────────┤
│ product_id UUID PK            │
│ sku         TEXT UNIQUE       │
│ quantity    INT               │
│ reserved_qty INT              │
│ version     BIGINT            │  ← optimistic locking
│ updated_at  TSTZ              │
└───────────────────────────────┘

┌──────────────────────────────────┐
│        notifications             │
├──────────────────────────────────┤
│ id UUID PK                       │
│ order_id UUID                    │
│ channel  VARCHAR(10)             │
│ recipient TEXT                   │
│ template TEXT                    │
│ status   VARCHAR(10)             │
│ sent_at  TSTZ                    │
│ error    TEXT                    │
└──────────────────────────────────┘
```

### Full DDL

```sql
-- ============================================================
-- Orders DB (used by Order Service)
-- ============================================================
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE orders (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id      UUID        NOT NULL,
  status           VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                               CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','FULFILLED')),
  items            JSONB       NOT NULL,
  total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  shipping_address JSONB       NOT NULL,
  idempotency_key  TEXT        UNIQUE,           -- prevents duplicate creation
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id ON orders(customer_id);
CREATE INDEX idx_orders_status      ON orders(status);
CREATE INDEX idx_orders_created_at  ON orders(created_at DESC);

-- Akka Persistence event journal (akka-persistence-jdbc)
CREATE TABLE order_events (
  ordering        BIGSERIAL   PRIMARY KEY,
  persistence_id  TEXT        NOT NULL,
  sequence_nr     BIGINT      NOT NULL,
  deleted         BOOLEAN     NOT NULL DEFAULT FALSE,
  writer          TEXT        NOT NULL,
  write_timestamp BIGINT      NOT NULL,
  adapter_manifest TEXT,
  event_ser_id    INT         NOT NULL,
  event_ser_manifest TEXT     NOT NULL,
  event_payload   BYTEA       NOT NULL,
  meta_ser_id     INT,
  meta_ser_manifest TEXT,
  meta_payload    BYTEA,
  UNIQUE (persistence_id, sequence_nr)
);

CREATE INDEX idx_event_journal_pid ON order_events(persistence_id, sequence_nr);

-- Akka snapshot store
CREATE TABLE order_snapshots (
  persistence_id  TEXT    NOT NULL,
  sequence_nr     BIGINT  NOT NULL,
  created         BIGINT  NOT NULL,
  snapshot_ser_id INT     NOT NULL,
  snapshot_ser_manifest TEXT NOT NULL,
  snapshot_payload BYTEA  NOT NULL,
  meta_ser_id     INT,
  meta_ser_manifest TEXT,
  meta_payload    BYTEA,
  PRIMARY KEY (persistence_id, sequence_nr)
);

-- ============================================================
-- Inventory DB (used by Inventory Service)
-- ============================================================
CREATE TABLE inventory (
  product_id   UUID        PRIMARY KEY,
  sku          TEXT        NOT NULL UNIQUE,
  name         TEXT        NOT NULL,
  quantity     INT         NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  reserved_qty INT         NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
  version      BIGINT      NOT NULL DEFAULT 0,  -- optimistic lock
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_reservations (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id     UUID        NOT NULL,
  product_id   UUID        NOT NULL REFERENCES inventory(product_id),
  quantity     INT         NOT NULL CHECK (quantity > 0),
  status       VARCHAR(10) NOT NULL DEFAULT 'ACTIVE'
                           CHECK (status IN ('ACTIVE','RELEASED','COMMITTED')),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_order ON inventory_reservations(order_id);

-- ============================================================
-- Notification DB (used by Notification Service)
-- ============================================================
CREATE TABLE notifications (
  id          UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id    UUID        NOT NULL,
  channel     VARCHAR(10) NOT NULL CHECK (channel IN ('EMAIL','SMS','PUSH')),
  recipient   TEXT        NOT NULL,
  template    TEXT        NOT NULL,
  payload     JSONB,
  status      VARCHAR(10) NOT NULL DEFAULT 'PENDING'
                          CHECK (status IN ('PENDING','DELIVERED','FAILED')),
  attempts    INT         NOT NULL DEFAULT 0,
  sent_at     TIMESTAMPTZ,
  error_msg   TEXT,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_order ON notifications(order_id);
CREATE INDEX idx_notifications_status ON notifications(status);
```

---

## 2.3a — Database Schema Migrations (Flyway)

### Why Flyway (not Liquibase)

| | Flyway | Liquibase |
|---|---|---|
| Format | Plain SQL (default) | XML / YAML / JSON / SQL |
| Spring Boot integration | Auto-configured — zero code | Requires bean setup |
| SBT plugin | `flyway-sbt` | `sbt-liquibase` (less maintained) |
| Learning curve | Low — just numbered SQL files | Higher — DSL + XML schema |
| Lock mechanism | `flyway_schema_history` table | `DATABASECHANGELOGLOCK` table |
| Rollback | Manual (Flyway Teams) / script | Built-in (XML changesets) |
| **Verdict** | ✅ Use this | Not needed for this project |

**Decision**: Flyway with plain SQL migrations. One Flyway config per service database (database-per-service pattern — each service owns its schema exclusively).

---

### Migration File Naming Convention

```
V{version}__{description}.sql          ← versioned (applied once, in order)
U{version}__{description}.sql          ← undo script (optional, Teams edition)
R__{description}.sql                   ← repeatable (re-run when checksum changes)

Examples:
  V1__create_orders_table.sql
  V2__create_akka_journal_tables.sql
  V3__add_return_columns_to_orders.sql
  V4__add_idempotency_key_index.sql
  R__create_views.sql                  ← reporting views, re-applied on change
```

**Rules enforced by Flyway**:
- Never modify a versioned file after it has been applied (checksum fails)
- To change schema → create a new `V{n+1}__...` migration
- All migrations run inside a transaction (Postgres supports DDL transactions)

---

### Directory Layout (per service)

```
order-service/
  src/main/resources/
    db/migration/
      V1__create_orders_table.sql
      V2__create_akka_journal_tables.sql
      V3__create_akka_snapshot_table.sql
      V4__add_return_columns.sql          ← from LLD 2.11 return flow
      V5__add_stale_pending_index.sql

inventory-service/
  src/main/resources/
    db/migration/
      V1__create_inventory_table.sql
      V2__create_inventory_reservations.sql
      V3__add_sku_index.sql

notification-service/
  src/main/resources/
    db/migration/
      V1__create_notifications_table.sql
      V2__add_notification_status_index.sql
```

---

### Integration — Order Service (Scala + SBT)

Flyway runs programmatically on `OrderServiceApp.scala` startup — **before** `ActorSystem` boots and before Slick accepts any query.

**`build.sbt` dependency**:
```scala
libraryDependencies += "org.flywaydb" % "flyway-core"           % "10.15.0"
libraryDependencies += "org.flywaydb" % "flyway-database-postgresql" % "10.15.0"
libraryDependencies += "org.postgresql" % "postgresql"           % "42.7.3"
```

**`OrderServiceApp.scala` — startup sequence**:
```scala
object OrderServiceApp extends App {

  // 1. Run Flyway migrations FIRST — before anything else starts
  runMigrations()

  // 2. Boot ActorSystem (Akka Persistence uses the migrated schema)
  implicit val system: ActorSystem[Nothing] =
    ActorSystem(Behaviors.empty, "order-system")

  // 3. Start HTTP server
  // ...

  private def runMigrations(): Unit = {
    val config = ConfigFactory.load()
    val flyway = Flyway.configure()
      .dataSource(
        config.getString("db.url"),
        config.getString("db.user"),
        config.getString("db.password")
      )
      .locations("classpath:db/migration")
      .baselineOnMigrate(true)          // safe for first-time deploy on existing DB
      .validateOnMigrate(true)          // fail fast if migration files are tampered
      .load()

    val result = flyway.migrate()
    println(s"Flyway: applied ${result.migrationsExecuted} migration(s)")
  }
}
```

**`application.conf` — DB config (used by both Flyway and Slick)**:
```hocon
db {
  url      = "jdbc:postgresql://postgres:5432/ops_orders"
  user     = ${DB_USER}          # injected from K8s Secret / env var
  password = ${DB_PASSWORD}      # injected from K8s Secret / env var
  driver   = "org.postgresql.Driver"
  hikari {
    maximumPoolSize = 20
    connectionTimeout = 5000
  }
}
```

---

### Integration — Inventory Service (Scala + SBT)

Identical approach to Order Service — Flyway runs first in `InventoryApp.scala`:
```scala
object InventoryApp extends App {
  runMigrations()   // same pattern as OrderServiceApp
  // ... boot Akka Streams pipeline
}
```

---

### Integration — Notification Service (Java + Spring Boot)

Spring Boot **auto-configures Flyway with zero code** when `flyway-core` is on the classpath.

**`pom.xml` dependency**:
```xml
<dependency>
  <groupId>org.flywaydb</groupId>
  <artifactId>flyway-core</artifactId>
  <!-- version managed by spring-boot-starter-parent -->
</dependency>
<dependency>
  <groupId>org.flywaydb</groupId>
  <artifactId>flyway-database-postgresql</artifactId>
</dependency>
```

**`application.yml`** — Spring Boot picks this up automatically:
```yaml
spring:
  datasource:
    url:      jdbc:postgresql://postgres:5432/ops_notifications
    username: ${DB_USER}
    password: ${DB_PASSWORD}
  flyway:
    enabled:             true
    locations:           classpath:db/migration
    baseline-on-migrate: true
    validate-on-migrate: true
    out-of-order:        false    # reject gaps in version numbers
```

Flyway runs before the Spring application context finishes starting — JPA entities are never accessed before migrations complete.

---

### Actual Migration Files

**`V1__create_orders_table.sql`** (Order Service):
```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE orders (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id      UUID        NOT NULL,
  status           VARCHAR(20) NOT NULL DEFAULT 'PENDING'
                               CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','FULFILLED',
                                                 'RETURN_REQUESTED','RETURNED')),
  items            JSONB       NOT NULL,
  total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  shipping_address JSONB       NOT NULL,
  idempotency_key  TEXT        UNIQUE,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_orders_customer_id  ON orders(customer_id);
CREATE INDEX idx_orders_status       ON orders(status);
CREATE INDEX idx_orders_created_at   ON orders(created_at DESC);
CREATE INDEX idx_orders_stale_pending ON orders(created_at)
  WHERE status = 'PENDING';
```

**`V2__create_akka_journal_tables.sql`** (Order Service — Akka Persistence):
```sql
-- akka-persistence-jdbc standard schema
CREATE TABLE order_events (
  ordering         BIGSERIAL   PRIMARY KEY,
  persistence_id   TEXT        NOT NULL,
  sequence_nr      BIGINT      NOT NULL,
  deleted          BOOLEAN     NOT NULL DEFAULT FALSE,
  writer           TEXT        NOT NULL,
  write_timestamp  BIGINT      NOT NULL,
  adapter_manifest TEXT,
  event_ser_id     INT         NOT NULL,
  event_ser_manifest TEXT      NOT NULL,
  event_payload    BYTEA       NOT NULL,
  meta_ser_id      INT,
  meta_ser_manifest TEXT,
  meta_payload     BYTEA,
  UNIQUE (persistence_id, sequence_nr)
);

CREATE INDEX idx_event_journal_pid ON order_events(persistence_id, sequence_nr);

CREATE TABLE order_snapshots (
  persistence_id       TEXT   NOT NULL,
  sequence_nr          BIGINT NOT NULL,
  created              BIGINT NOT NULL,
  snapshot_ser_id      INT    NOT NULL,
  snapshot_ser_manifest TEXT  NOT NULL,
  snapshot_payload     BYTEA  NOT NULL,
  meta_ser_id          INT,
  meta_ser_manifest    TEXT,
  meta_payload         BYTEA,
  PRIMARY KEY (persistence_id, sequence_nr)
);
```

**`V3__add_return_columns.sql`** (Order Service — return flow added later):
```sql
-- Additive change: new columns for return lifecycle
ALTER TABLE orders
  ADD COLUMN cancelled_reason    TEXT,
  ADD COLUMN cancelled_at        TIMESTAMPTZ,
  ADD COLUMN return_requested_at TIMESTAMPTZ,
  ADD COLUMN return_reason       TEXT,
  ADD COLUMN returned_at         TIMESTAMPTZ,
  ADD COLUMN refund_amount       NUMERIC(12,2);

CREATE INDEX idx_orders_return_status ON orders(status)
  WHERE status = 'RETURN_REQUESTED';
```

**`V1__create_inventory_tables.sql`** (Inventory Service):
```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE inventory (
  product_id    UUID        PRIMARY KEY,
  sku           TEXT        NOT NULL UNIQUE,
  name          TEXT        NOT NULL,
  quantity      INT         NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  reserved_qty  INT         NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
  version       BIGINT      NOT NULL DEFAULT 0,
  updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE inventory_reservations (
  id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id     UUID        NOT NULL,
  product_id   UUID        NOT NULL REFERENCES inventory(product_id),
  quantity     INT         NOT NULL CHECK (quantity > 0),
  status       VARCHAR(10) NOT NULL DEFAULT 'ACTIVE'
                           CHECK (status IN ('ACTIVE','RELEASED','COMMITTED')),
  created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_order   ON inventory_reservations(order_id);
CREATE INDEX idx_reservations_product ON inventory_reservations(product_id)
  WHERE status = 'ACTIVE';
```

**`V1__create_notifications_table.sql`** (Notification Service):
```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE notifications (
  id         UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id   UUID        NOT NULL,
  channel    VARCHAR(10) NOT NULL CHECK (channel IN ('EMAIL','SMS','PUSH')),
  recipient  TEXT        NOT NULL,
  template   TEXT        NOT NULL,
  payload    JSONB,
  status     VARCHAR(10) NOT NULL DEFAULT 'PENDING'
                         CHECK (status IN ('PENDING','DELIVERED','FAILED')),
  attempts   INT         NOT NULL DEFAULT 0,
  sent_at    TIMESTAMPTZ,
  error_msg  TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_order  ON notifications(order_id);
CREATE INDEX idx_notifications_status ON notifications(status)
  WHERE status IN ('PENDING','FAILED');
```

---

### Flyway in Kubernetes — Init Container Pattern

For production K8s, migrations run in an **init container** before the main service pod starts. This prevents race conditions when multiple pods start simultaneously and all try to migrate at once (Flyway's schema lock handles this, but init containers are cleaner):

```yaml
# order-service-deployment.yaml
spec:
  template:
    spec:
      initContainers:
        - name: flyway-migrate
          image: flyway/flyway:10-alpine
          args:
            - -url=jdbc:postgresql://postgres:5432/ops_orders
            - -user=$(DB_USER)
            - -password=$(DB_PASSWORD)
            - -locations=filesystem:/flyway/sql
            - migrate
          envFrom:
            - secretRef: { name: db-credentials }
          volumeMounts:
            - name: migrations
              mountPath: /flyway/sql
      volumes:
        - name: migrations
          configMap: { name: order-service-migrations }  # or baked into image
      containers:
        - name: order-service
          # ... main app
```

**Alternative (simpler)**: bake migrations into the app image and let Flyway run on startup (already shown in `OrderServiceApp.scala`). The init container approach is preferred in prod for clean separation but either works.

---

### Migration Safety Rules

| Rule | Why |
|------|-----|
| Never `DROP COLUMN` in a migration without a prior deploy that stops reading that column | Zero-downtime deploys — old pods still running read the column |
| Never `ALTER TABLE ... ALTER COLUMN TYPE` on a large table in one migration | Takes an `ACCESS EXCLUSIVE` lock; table is inaccessible during migration. Use shadow column + backfill + rename in 3 separate migrations |
| Never rename a column directly | Same lock issue. Use additive approach: add new column, backfill, update code, drop old column in next release |
| `baseline-on-migrate = true` | Safe for first deploy onto an existing manually-created schema |
| `validate-on-migrate = true` | Detects if anyone modified a migration file after it ran — fails fast before corrupting data |
| All migrations in a transaction | Postgres supports transactional DDL — failed migration rolls back cleanly |

---

## 2.4 Order Service — Class Design & State Machine

### Order State Machine

> Full expanded state machine with RETURN_REQUESTED and RETURNED states is in **LLD Section 2.11**.

```
                    ┌───────────────────────────────────────────────────────┐
                    │                                                       │
         CreateOrder│             CancelOrder (manual or SLA timeout)       │
                    ▼                    │                                  │
              ┌─────────┐   ConfirmOrder │             ┌──────────────┐     │
   ──────────►│ PENDING │──────────────►┤  ┌─────────►│  CONFIRMED   │     │
              └─────────┘               │  │           └──────┬───────┘     │
                    │                   │  │                  │             │
                    │ CancelOrder        │  │         FulfillOrder  ReturnOrder│
                    ▼                   ▼  │                  ▼             │
              ┌───────────┐     ┌──────────┴────┐    ┌──────────────┐      │
              │ CANCELLED │     │  CANCELLED    │    │  FULFILLED   │      │
              └───────────┘     └───────────────┘    └──────┬───────┘      │
                                                            │               │
                                                      RequestReturn         │
                                                            ▼               │
                                                   ┌─────────────────┐     │
                                                   │ RETURN_REQUESTED│─────┘
                                                   └────────┬────────┘
                                                            │
                                            ApproveReturn   │   RejectReturn
                                                    ▼       │       ▼
                                             ┌──────────┐   │  ┌──────────┐
                                             │ RETURNED │   │  │FULFILLED │
                                             └──────────┘   │  └──────────┘
```

### Class Design (Scala — Order Service)

```scala
// ── Domain ──────────────────────────────────────────────────────────

sealed trait OrderStatus
object OrderStatus {
  case object Pending         extends OrderStatus
  case object Confirmed       extends OrderStatus
  case object Cancelled       extends OrderStatus
  case object Fulfilled       extends OrderStatus
  case object ReturnRequested extends OrderStatus  // new
  case object Returned        extends OrderStatus  // new
}

case class ItemLine(productId: String, quantity: Int, unitPrice: BigDecimal)
case class ShippingAddress(street: String, city: String, country: String, zip: String)
case class Order(
  id:              String,
  customerId:      String,
  items:           List[ItemLine],
  status:          OrderStatus,
  totalAmount:     BigDecimal,
  shippingAddress: ShippingAddress,
  createdAt:       Instant,
  updatedAt:       Instant
)

// ── Commands (input to actor) ──────────────────────────────────────

sealed trait OrderCommand
case class CreateOrder(
  orderId:        String,
  customerId:     String,
  items:          List[ItemLine],
  shippingAddress: ShippingAddress,
  replyTo:        ActorRef[OrderReply]
) extends OrderCommand

case class ConfirmOrder(orderId: String, replyTo: ActorRef[OrderReply]) extends OrderCommand
case class CancelOrder(orderId: String, reason: String, replyTo: ActorRef[OrderReply]) extends OrderCommand
case class FulfillOrder(orderId: String, replyTo: ActorRef[OrderReply]) extends OrderCommand
case class GetOrder(orderId: String, replyTo: ActorRef[OrderReply]) extends OrderCommand

// ── Events (what gets persisted) ──────────────────────────────────

sealed trait OrderEvent
case class OrderCreated(orderId: String, customerId: String, items: List[ItemLine],
                        shippingAddress: ShippingAddress, totalAmount: BigDecimal,
                        at: Instant) extends OrderEvent
case class OrderConfirmed(orderId: String, at: Instant) extends OrderEvent
case class OrderCancelled(orderId: String, reason: String, at: Instant) extends OrderEvent
case class OrderFulfilled(orderId: String, at: Instant) extends OrderEvent
case class ReturnRequested(orderId: String, reason: String,
                           items: List[ItemLine], at: Instant) extends OrderEvent  // new
case class ReturnApproved(orderId: String, at: Instant)  extends OrderEvent        // new
case class ReturnRejected(orderId: String, reason: String, at: Instant) extends OrderEvent // new

// ── Actor State ───────────────────────────────────────────────────

case class OrderState(order: Option[Order] = None) {
  def applyEvent(event: OrderEvent): OrderState = event match {
    case e: OrderCreated   => copy(order = Some(Order(...)))
    case e: OrderConfirmed => copy(order = order.map(_.copy(status = Confirmed)))
    case e: OrderCancelled => copy(order = order.map(_.copy(status = Cancelled)))
    case e: OrderFulfilled => copy(order = order.map(_.copy(status = Fulfilled)))
  }
}

// ── Replies ───────────────────────────────────────────────────────

sealed trait OrderReply
case class OrderCreatedReply(order: Order)  extends OrderReply
case class OrderUpdatedReply(order: Order)  extends OrderReply
case class OrderNotFound(orderId: String)   extends OrderReply
case class OrderRejected(reason: String)    extends OrderReply
case class OrderDetails(order: Order)       extends OrderReply
```

### Sequence Diagram — Create Order

```
Client        Gateway        OrderActor         Postgres       Kafka
  │              │               │                  │             │
  │ POST /orders │               │                  │             │
  │─────────────►│               │                  │             │
  │              │ validate JWT  │                  │             │
  │              │ check idempotency key (Redis)     │             │
  │              │ HTTP POST /orders                 │             │
  │              │──────────────►│                  │             │
  │              │               │ CreateOrder cmd  │             │
  │              │               │ validate state   │             │
  │              │               │ persist OrderCreated           │
  │              │               │─────────────────►│             │
  │              │               │ reply OrderCreatedReply        │
  │              │               │◄─────────────────│             │
  │              │               │ publish event    │             │
  │              │               │──────────────────────────────►│
  │              │ 201 Created   │                  │             │
  │◄─────────────│               │                  │             │
```

---

## 2.5 Inventory Service — Stream Pipeline Design

### Akka Streams Graph

```
                  ┌─────────────────────────────────────────────────────────┐
                  │                  Inventory Stream                        │
                  │                                                          │
Kafka             │  Source              Flow                   Sink         │
order.created ───►│  CommittableSource   │                      │           │
                  │  (parallelism: 4)    │                      │           │
                  │       │             ▼                       │           │
                  │       │     deserialize JSON                │           │
                  │       │             │                       │           │
                  │       │             ▼                       │           │
                  │       │     checkRedisCache                 │           │
                  │       │     (HIT: use cached qty)           │           │
                  │       │     (MISS: SELECT FROM postgres)    │           │
                  │       │             │                       │           │
                  │       │             ▼                       │           │
                  │       │     reserveStock (ATOMIC)           │           │
                  │       │     UPDATE inventory                │           │
                  │       │     SET reserved_qty = reserved_qty + n        │
                  │       │     WHERE version = $v              │           │
                  │       │     AND quantity - reserved_qty >= n│           │
                  │       │     RETURNING *                     │           │
                  │       │             │                       │           │
                  │       │     ┌───────┴──────────┐            │           │
                  │       │     │ SUCCESS           │ FAILURE    │           │
                  │       │     ▼                  ▼            │           │
                  │       │  DEL redis cache  publish           │           │
                  │       │  publish           order.cancel     │           │
                  │       │  inventory.updated .requested       │           │
                  │       │     │                  │            │           │
                  │       │     └──────────────────┘            │           │
                  │       │             │                       │           │
                  │       │             ▼                       │           │
                  │       │      commitOffset                   │           │
                  │       │──────────────────────────────────►  │           │
                  │                                             Committer   │
                  └─────────────────────────────────────────────────────────┘
```

### Concurrency & Backpressure Config

```scala
// Parallelism: 4 concurrent DB ops per partition
// Max in-flight: 100 uncommitted offsets
// Commit interval: every 5s or 1000 messages

val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
  .withGroupId("inventory-service")
  .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  .withProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100")

val committerSettings = CommitterSettings(system)
  .withMaxBatch(1000)
  .withMaxInterval(5.seconds)
```

---

## 2.6 Notification Service — Actor Design

### Actor Hierarchy

```
ActorSystem[NotificationCommand]
      │
      └── NotificationSupervisor
               │
               ├── EmailNotificationActor  (pool of 4)
               ├── SmsNotificationActor    (pool of 2)
               └── PushNotificationActor   (pool of 2)
```

### Class Design (Java — Notification Service)

```java
// Commands
public sealed interface NotificationCommand permits
    SendOrderConfirmation, SendInventoryAlert,
    SendOrderCancellation, NotificationFailed;

public record SendOrderConfirmation(
    String orderId, String customerId,
    String email, String phone,
    BigDecimal totalAmount
) implements NotificationCommand {}

public record SendInventoryAlert(
    String orderId, String productId,
    String reason
) implements NotificationCommand {}

// Notification routing behavior
public class NotificationRouter {
  public static Behavior<NotificationCommand> create(
      ActorRef<EmailNotificationActor.Command> emailActor,
      ActorRef<SmsNotificationActor.Command>   smsActor
  ) {
    return Behaviors.receive(NotificationCommand.class)
      .onMessage(SendOrderConfirmation.class, cmd -> {
        emailActor.tell(new EmailNotificationActor.SendEmail(
            cmd.email(), "ORDER_CONFIRMED", Map.of("orderId", cmd.orderId())));
        smsActor.tell(new SmsNotificationActor.SendSms(
            cmd.phone(), "Order " + cmd.orderId() + " confirmed!"));
        return Behaviors.same();
      })
      .onMessage(SendOrderCancellation.class, cmd -> {
        emailActor.tell(...);
        return Behaviors.same();
      })
      .build();
  }
}
```

---

## 2.7 API Gateway — Filter Chain

```
Inbound Request
      │
      ▼
┌─────────────────────────────────────────────────────┐
│ 1. RateLimitFilter                                   │
│    Redis INCR ops:client:{ip} EX 60                  │
│    If > 100/min → 429 Too Many Requests              │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│ 2. JwtAuthFilter                                     │
│    Extract Bearer token from Authorization header    │
│    Verify RS256 signature against public key         │
│    Validate: exp, iss, aud                           │
│    Reject 401 if invalid; extract sub → X-User-Id   │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│ 3. IdempotencyFilter (POST requests only)            │
│    Read Idempotency-Key header                       │
│    Check Redis: ops:idempotency:{key}                │
│    HIT → return cached 201 response (no forwarding)  │
│    MISS → forward; cache response with 24h TTL       │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│ 4. RequestLoggingFilter                              │
│    Log: traceId, method, path, userId, duration      │
│    Inject X-Trace-Id header (from OTel context)      │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────┐
│ 5. Spring Cloud Gateway Route Matching               │
│    CircuitBreaker per route (Resilience4j)           │
│    Open after 5 failures in 10s window               │
│    Half-open after 30s → try 1 request               │
│    Fallback: FallbackController → 503 JSON response  │
└──────────────────────┬──────────────────────────────┘
                       │
                       ▼
             Downstream Service
```

---

## 2.8 Caching Strategy

### Redis Key Design

```
ops:idempotency:{idempotencyKey}    TTL: 24h    Value: cached HTTP response JSON
ops:inventory:{productId}           TTL: 300s   Value: InventoryItem JSON
ops:ratelimit:{clientIp}            TTL: 60s    Value: counter (INCR)
ops:session:{userId}                TTL: 1h     Value: JWT claims (optional)
```

### Cache Invalidation Rules

| Event | Action |
|-------|--------|
| `inventory.updated` consumed | `DEL ops:inventory:{productId}` |
| Inventory PUT endpoint called | `DEL ops:inventory:{productId}` |
| Redis eviction (allkeys-lru) | Transparent — next read re-fetches from Postgres |
| Inventory item deleted | `DEL ops:inventory:{productId}` |

**Never cache**: order status (must be real-time from actor/DB), DLT messages, payment data.

---

## 2.9 Error Handling & Resilience

### Resilience Layers

```
┌─────────────────────────────────────────────┐
│ Layer 1: Input Validation (Gateway)          │
│   @Valid + Bean Validation                   │
│   Returns 400 with field-level error details │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│ Layer 2: Circuit Breaker (Gateway)           │
│   Resilience4j per route                     │
│   Prevents cascade failure                   │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│ Layer 3: Akka Actor Supervision (Order Svc)  │
│   OneForOneStrategy                          │
│   Restart on RuntimeException (max 10×/min)  │
│   Stop on PersistenceFailureException        │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│ Layer 4: Optimistic Locking (Inventory Svc)  │
│   Retry up to 3× on version conflict         │
│   Then emit rejection event                  │
└────────────────────┬────────────────────────┘
                     │
┌────────────────────▼────────────────────────┐
│ Layer 5: Kafka Retry + DLT (All consumers)   │
│   3 retries with exponential backoff         │
│   Failed messages → *.DLT topic              │
│   DLT monitored — alert on non-zero lag      │
└─────────────────────────────────────────────┘
```

### Error Response Contract (all services)

```json
{
  "error": {
    "code":      "INSUFFICIENT_STOCK",
    "message":   "Product prod-001 has only 1 unit available, requested 2",
    "traceId":   "4bf92f3577b34da6a3ce929d0e0e4736",
    "timestamp": "2026-06-29T10:30:00Z",
    "path":      "/api/orders"
  }
}
```

---

## 2.10 — Layered Architecture: Endpoint → Service → Repository

Every service strictly enforces **3 layers with one-way dependency**: Endpoint depends on Service; Service depends on Repository. No layer skips or reverse-calls another.

```
┌─────────────────────────────────────────────────────────────────────┐
│                         ENDPOINT LAYER                              │
│  • Parse HTTP request → validate → call Service                     │
│  • Serialize Service result → HTTP response                         │
│  • NO business logic. NO DB calls. NO Kafka calls.                  │
│  • Stateless — safe to run N replicas with zero coordination        │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  passes: validated DTO / command
                               │  receives: domain result / error
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                         SERVICE LAYER                               │
│  • Owns ALL business logic and orchestration                        │
│  • Calls Repository for reads/writes                                │
│  • Sends Kafka events after successful state changes                │
│  • Returns domain objects — never HTTP types, never raw SQL rows    │
│  • Can be called by: HTTP endpoint, Kafka consumer, scheduled job   │
└──────────────────────────────┬──────────────────────────────────────┘
                               │  passes: query params / domain objects
                               │  receives: domain objects / Option/Either
                               ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       REPOSITORY LAYER                              │
│  • ONLY data access: SQL reads/writes via Slick or Spring Data JPA  │
│  • Cache-aside logic lives here (Redis read → DB fallback)          │
│  • Returns domain objects — never exposes SQL row types upward      │
│  • NO business logic. NO Kafka calls. NO HTTP calls.                │
└─────────────────────────────────────────────────────────────────────┘
```

---

### Order Service — Full Layer Breakdown

```
order-service/src/main/scala/com/ops/order/
│
├── api/                        ◄─ ENDPOINT LAYER
│   ├── OrderController.scala   │  Akka HTTP route DSL
│   │   • POST /orders          │  • unmarshal JSON → CreateOrderRequest (DTO)
│   │   • GET  /orders/{id}     │  • @Valid equivalent → validate fields
│   │   • GET  /orders          │  • call OrderService.createOrder(cmd)
│   │   • DELETE /orders/{id}   │  • marshal OrderResponse → JSON + HTTP status
│   └── dto/
│       ├── CreateOrderRequest.scala   # Input DTO: validated fields only
│       ├── OrderResponse.scala        # Output DTO: what clients see
│       └── OrderListResponse.scala    # Paginated response wrapper
│
├── service/                    ◄─ SERVICE LAYER
│   ├── OrderService.scala      │  • def createOrder(cmd): Future[OrderResponse]
│   │                           │  • def getOrder(id): Future[Option[OrderResponse]]
│   │                           │  • def cancelOrder(id, reason): Future[OrderResponse]
│   │                           │  • validates business rules (e.g., can't cancel FULFILLED)
│   │                           │  • calls OrderActor (event sourcing) via AskPattern
│   │                           │  • calls OrderEventProducer after actor confirms
│   └── OrderServiceImpl.scala  │  (concrete implementation, interface for testing)
│
├── actor/                      ◄─ SERVICE LAYER (event-sourced state machine)
│   ├── OrderActor.scala        │  Akka Typed EventSourcedBehavior
│   │                           │  • Receives OrderCommand from OrderService
│   │                           │  • Validates command against current state
│   │                           │  • Persists OrderEvent to journal
│   │                           │  • Updates in-memory state
│   │                           │  • Replies to OrderService
│   └── OrderSupervisor.scala   │  Spawns/routes to per-order actors
│
├── repository/                 ◄─ REPOSITORY LAYER
│   ├── OrderRepository.scala   │  trait: find, save, updateStatus, findByCustomer
│   ├── OrderRepositoryImpl.scala│  Slick implementation
│   │                           │  • Uses HikariCP connection pool (max 20)
│   │                           │  • All queries are parameterized (no interpolation)
│   │                           │  • Returns Future[Option[Order]] — never null
│   └── OrderSchema.scala       │  Slick table mapping
│
├── kafka/
│   └── OrderEventProducer.scala ◄─ infrastructure (called FROM service layer only)
│
├── domain/                     ◄─ PURE DOMAIN (no layer — shared by all layers)
│   ├── Order.scala
│   ├── OrderStatus.scala
│   ├── OrderCommand.scala
│   └── OrderEvent.scala
│
└── infra/
    └── DatabaseConfig.scala    ◄─ infrastructure (HikariCP pool config)
```

**Layer contract — Order Service**:

```scala
// ENDPOINT → SERVICE contract
trait OrderService {
  def createOrder(req: CreateOrderRequest): Future[Either[ServiceError, OrderResponse]]
  def getOrder(id: String):                 Future[Option[OrderResponse]]
  def listOrders(page: Int, size: Int):     Future[PagedResponse[OrderResponse]]
  def cancelOrder(id: String, reason: String): Future[Either[ServiceError, OrderResponse]]
}

// SERVICE → REPOSITORY contract
trait OrderRepository {
  def save(order: Order):                      Future[Order]
  def findById(id: String):                    Future[Option[Order]]
  def findByCustomer(customerId: String,
                     page: Int, size: Int):    Future[(List[Order], Int)]
  def updateStatus(id: String,
                   status: OrderStatus):       Future[Option[Order]]
}
```

---

### Inventory Service — Full Layer Breakdown

```
inventory-service/src/main/scala/com/ops/inventory/
│
├── api/                        ◄─ ENDPOINT LAYER
│   ├── InventoryController.scala
│   │   • GET  /inventory/{productId}       → call InventoryService.getItem
│   │   • PUT  /inventory/{productId}       → call InventoryService.updateStock
│   │   • POST /inventory/reserve           → call InventoryService.reserve
│   │   • POST /inventory/release           → call InventoryService.release
│   └── dto/
│       ├── InventoryResponse.scala
│       ├── ReserveRequest.scala
│       └── UpdateStockRequest.scala
│
├── service/                    ◄─ SERVICE LAYER
│   ├── InventoryService.scala  │  • def getItem(productId): Future[Option[InventoryResponse]]
│   │                           │  • def reserve(req): Future[Either[ServiceError, Unit]]
│   │                           │    - checks available = quantity - reserved_qty
│   │                           │    - calls repo.reserveWithOptimisticLock(...)
│   │                           │    - on success: calls producer.publishUpdated(...)
│   │                           │    - on failure: calls producer.publishRejection(...)
│   └── InventoryServiceImpl.scala
│
├── stream/                     ◄─ SERVICE LAYER (reactive Kafka consumer)
│   └── InventoryStream.scala   │  • Kafka Source → call InventoryService.reserve
│                               │  • Service is the single entry point — stream just feeds it
│
├── repository/                 ◄─ REPOSITORY LAYER
│   ├── InventoryRepository.scala      # trait
│   ├── InventoryRepositoryImpl.scala  # Slick impl with optimistic locking
│   └── InventoryCache.scala           # Redis cache-aside (wraps InventoryRepository)
│       • get(productId): check Redis → fallback to DB → cache result
│       • invalidate(productId): DEL key after write
│
└── domain/
    ├── InventoryItem.scala
    └── ReservationResult.scala   # Success(remaining) | Failure(reason)
```

**Layer contract — Inventory Service**:

```scala
trait InventoryService {
  def getItem(productId: String):            Future[Option[InventoryResponse]]
  def reserve(req: ReserveRequest):          Future[Either[InsufficientStock, ReservationResult]]
  def release(orderId: String):              Future[Unit]
  def updateStock(productId: String,
                  delta: Int):              Future[Either[ServiceError, InventoryResponse]]
}

trait InventoryRepository {
  def findById(productId: String):           Future[Option[InventoryItem]]
  def reserveWithLock(productId: String,
                      qty: Int,
                      version: Long):        Future[Either[OptimisticLockFailed, InventoryItem]]
  def releaseReservation(orderId: String):   Future[Unit]
  def updateQuantity(productId: String,
                     delta: Int):            Future[Option[InventoryItem]]
}
```

---

### Notification Service — Full Layer Breakdown

```
notification-service/src/main/java/com/ops/notification/
│
├── controller/                 ◄─ ENDPOINT LAYER
│   └── NotificationController.java
│       • GET /notifications?orderId=...   → NotificationService.getByOrder
│       • GET /notifications/{id}          → NotificationService.getById
│
├── service/                    ◄─ SERVICE LAYER
│   ├── NotificationService.java           # interface
│   ├── NotificationServiceImpl.java       # implementation
│   │   • sendOrderConfirmation(event)
│   │     - builds NotificationRecord
│   │     - routes to NotificationActor
│   │     - saves to DB via NotificationRepository
│   │     - publishes notif.sent event
│   └── NotificationFactory.java          # builds channel-specific payloads
│
├── consumer/                   ◄─ SERVICE LAYER entry point (Kafka-driven)
│   └── NotificationConsumer.java
│       @KafkaListener → calls NotificationService (NEVER repository directly)
│
├── actor/                      ◄─ SERVICE LAYER (async dispatch)
│   ├── NotificationRouter.java           # routes to channel-specific actors
│   ├── EmailNotificationActor.java       # sends via JavaMailSender
│   └── SmsNotificationActor.java         # sends via Twilio stub
│
├── repository/                 ◄─ REPOSITORY LAYER
│   ├── NotificationRepository.java       # Spring Data JPA interface
│   └── NotificationRecord.java           # @Entity JPA mapping
│       (save, findByOrderId, findById, updateStatus)
│
└── dto/
    ├── NotificationResponse.java
    └── NotificationListResponse.java
```

**Layer contract — Notification Service**:

```java
public interface NotificationService {
    NotificationResponse sendOrderConfirmation(OrderCreatedEvent event);
    NotificationResponse sendOrderCancellation(OrderCancelledEvent event);
    Optional<NotificationResponse> getById(String id);
    List<NotificationResponse> getByOrderId(String orderId);
}

// Repository — Spring Data JPA (auto-implemented)
public interface NotificationRepository extends JpaRepository<NotificationRecord, UUID> {
    List<NotificationRecord> findByOrderId(UUID orderId);
    List<NotificationRecord> findByStatus(String status);
}
```

---

### Scalability Rules Per Layer

```
┌────────────────────────┬─────────────────────────────────────────────────────┐
│ Layer                  │ Scalability Mechanism                               │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ Endpoint Layer         │ • Fully stateless — add pods freely                 │
│ (HTTP Controllers)     │ • Reactive (WebFlux / Akka HTTP) = non-blocking I/O  │
│                        │ • No session state stored here; JWT is self-contained│
│                        │ • K8s HPA on CPU; target 70%                        │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ Service Layer          │ Order Service: one Akka actor per orderId            │
│ (Business Logic)       │ → actors are lightweight (millions possible)         │
│                        │ → actor state is fully in-memory; journal in Postgres│
│                        │ → no shared mutable state between actors             │
│                        │                                                     │
│                        │ Inventory Service: Akka Streams pipeline             │
│                        │ → parallelism = 4 per partition, 3 partitions = 12  │
│                        │ → KEDA scales pods when Kafka consumer lag grows     │
│                        │                                                     │
│                        │ Notification Service: Akka actor pool                │
│                        │ → pool of 4 email + 2 SMS actors per pod             │
│                        │ → fire-and-forget dispatch from consumer             │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ Repository Layer       │ • HikariCP pool per service (max 20 connections)     │
│ (Data Access)          │ • Slick uses async DB execution context (no threads) │
│                        │ • Redis cache-aside: reduces DB load by ~80% for hot │
│                        │   inventory reads                                    │
│                        │ • Optimistic locking on inventory: no DB row locks;  │
│                        │   concurrent updates retry instead of queue          │
│                        │ • Postgres read replica for reporting queries        │
│                        │ • Connection pool shared within pod, NOT across pods │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ Kafka (Event Bus)      │ • 3 partitions per hot topic = 3 parallel consumers  │
│                        │ • Scale consumers = add pods (Kafka rebalances)      │
│                        │ • DLT prevents poison-pill messages blocking the bus  │
│                        │ • Idempotency keys prevent duplicate processing       │
│                        │   when consumers restart and re-read offsets          │
├────────────────────────┼─────────────────────────────────────────────────────┤
│ Database               │ • Postgres primary for writes                        │
│ (Persistence)          │ • Postgres read replica for GET /orders (reporting)  │
│                        │ • JSONB for items = schema flexibility without joins  │
│                        │ • TIMESTAMPTZ indexes for time-range queries          │
│                        │ • Akka Persistence snapshots every 100 events:        │
│                        │   prevents long journal replay on actor restart       │
└────────────────────────┴─────────────────────────────────────────────────────┘
```

---

### Data Flow Through Layers — POST /api/orders

```
  Client
    │  POST /api/orders  {items, customerId, idempotencyKey}
    ▼
┌──────────────────────────────────────────────────────────────────────────┐
│ ENDPOINT LAYER — OrderController                                         │
│  1. unmarshal JSON → CreateOrderRequest                                  │
│  2. validate: @NotEmpty items, @NotNull customerId, positive prices      │
│  3. extract userId from JWT claims (added by gateway)                    │
│  4. call orderService.createOrder(request)  ──────────────────────────► │
└──────────────────────────────────────────────────────────────────────────┘
                                                                           │
┌──────────────────────────────────────────────────────────────────────────┘
│ SERVICE LAYER — OrderServiceImpl                                         │
│  1. check idempotency: redis GET ops:idempotency:{key}                   │
│     → HIT: return cached OrderResponse (skip all below)                 │
│  2. build CreateOrder command from request DTO                           │
│  3. ask OrderSupervisor ! CreateOrder(cmd)                               │
│     (Akka AskPattern, timeout 5s)                                        │
│  4. actor validates: "no existing order with this id"                    │
│  5. actor persists OrderCreated to journal (Postgres)                    │
│  6. actor replies OrderCreatedReply                                      │
│  7. service calls eventProducer.publish(OrderCreatedEvent) [async]       │
│  8. service caches response in Redis: SET ops:idempotency:{key} EX 86400 │
│  9. return Right(OrderResponse(orderId, status=PENDING, ...))           │
│  call orderRepository.save(order) ────────────────────────────────────► │
└──────────────────────────────────────────────────────────────────────────┘
                                                                           │
┌──────────────────────────────────────────────────────────────────────────┘
│ REPOSITORY LAYER — OrderRepositoryImpl                                   │
│  1. Slick INSERT INTO orders VALUES (...)                                │
│  2. returns Order domain object (maps DB row → case class)               │
│  3. HikariCP returns connection to pool after commit                     │
└──────────────────────────────────────────────────────────────────────────┘
    │
    ▼  Future[Order] propagates back up through Service → Endpoint
    │
  Client ← 201 { orderId, status: "PENDING", totalAmount, createdAt }
```

---

### Anti-Patterns Explicitly Banned

| Anti-Pattern | Why Banned | Correct Approach |
|---|---|---|
| Controller calling Repository directly | Skips business logic, impossible to test | Controller → Service → Repository only |
| Service returning `ResultSet` or Slick `Query` type | Leaks DB abstraction | Repository maps to domain objects first |
| Repository containing `if (status == CONFIRMED)` logic | Business rule in wrong layer | Move to Service layer |
| Actor/Service calling `Thread.sleep` or blocking ops | Blocks Akka dispatcher thread, kills throughput | Use `Future`, `mapAsync`, or `Behaviors.withTimers` |
| Shared mutable state between replicas (e.g., static `HashMap`) | Breaks horizontal scaling | Use Redis or Postgres for shared state |
| Fat DTO passed between all layers | Couples layers together | Separate: RequestDTO → domain Command → domain Object → ResponseDTO |
| Connection per request (no pooling) | Exhausts DB connections at load | HikariCP with configured `maximumPoolSize` |

---

## 2.11 — Order Lifecycle: Pending / Confirmed / Cancelled / Returned

### Complete Order State Machine (expanded)

```
                                                              ┌──────────────────────┐
                     CreateOrder                              │  RETURN_REQUESTED    │
  ──────────────────────────────►                             └──────────┬───────────┘
                                 │                                       │  ApproveReturn
                                 ▼                                       │
                           ┌───────────┐   ConfirmOrder          ┌──────▼────────────┐
                           │  PENDING  │──────────────────────►  │    CONFIRMED      │
                           └─────┬─────┘  (stock reserved)       └──────┬────────────┘
                                 │                                       │
                    ┌────────────┤ CancelOrder                   FulfillOrder
                    │            │ (manual or SLA timeout)               │
                    │            ▼                                        ▼
                    │      ┌───────────┐                        ┌────────────────┐
                    │      │ CANCELLED │                        │   FULFILLED    │
                    │      └─────┬─────┘                        └───────┬────────┘
                    │            │ release inventory                     │
                    │            │ notify customer                       │ ReturnOrder
                    │            ▼                                       ▼
                    │      ┌───────────────────────────────────────────────────┐
                    │      │          RETURN_REQUESTED                          │
                    └─────►│  (only from FULFILLED, not CANCELLED)              │
                           └───────────────────┬───────────────────────────────┘
                                               │
                             ┌─────────────────┴──────────────────┐
                             ▼                                     ▼
                      ApproveReturn                          RejectReturn
                             │                                     │
                             ▼                                     ▼
                      ┌──────────────┐                    ┌──────────────┐
                      │   RETURNED   │                    │   FULFILLED  │ (stays)
                      └──────────────┘                    └──────────────┘
                  release inventory
                  trigger refund event
                  notify customer

Terminal states: CANCELLED, RETURNED (after approved), FULFILLED (if return rejected)
```

**Allowed transitions only** — actor enforces no illegal state jumps:

| From | Command | To | Guard |
|------|---------|-----|-------|
| (none) | CreateOrder | PENDING | — |
| PENDING | ConfirmOrder | CONFIRMED | stock reserved |
| PENDING | CancelOrder | CANCELLED | any reason |
| PENDING | SlaTimeout | CANCELLED | age > 15 min (background job) |
| CONFIRMED | FulfillOrder | FULFILLED | shipped signal received |
| CONFIRMED | CancelOrder | CANCELLED | before shipment only |
| FULFILLED | RequestReturn | RETURN_REQUESTED | within 30-day window |
| RETURN_REQUESTED | ApproveReturn | RETURNED | ops/auto approval |
| RETURN_REQUESTED | RejectReturn | FULFILLED | outside policy |

---

### Scenario 1: PENDING Orders — SLA Timeout & Stale Cleanup

**Problem**: Orders stuck in PENDING (e.g. Kafka down, inventory service unavailable) must not block stock forever and must be surfaced to the customer.

**Design**:

```
┌──────────────────────────────────────────────────────────────────┐
│  PendingOrderTimeoutJob (Akka Timers / scheduled Akka Streams)   │
│                                                                  │
│  Every 1 minute:                                                 │
│  1. SELECT id FROM orders                                        │
│     WHERE status = 'PENDING'                                     │
│     AND created_at < now() - INTERVAL '15 minutes'              │
│                                                                  │
│  2. For each stale order:                                        │
│     → send CancelOrder(id, reason="SLA_TIMEOUT") to OrderActor   │
│     → actor transitions PENDING → CANCELLED                      │
│     → publish order.cancelled Kafka event                        │
│     → Inventory Service: no reservation to release (stock never  │
│       locked for PENDING — only locked on CONFIRMED)             │
│     → Notification Service: "Your order expired" email/SMS       │
└──────────────────────────────────────────────────────────────────┘
```

**Key design decision**: Inventory is **reserved on CONFIRMED, not PENDING**. This means:
- A PENDING order times out → nothing to unlock in inventory
- Stock only gets locked when `InventoryService.reserve()` succeeds
- Prevents ghost reservations from stuck orders

```scala
// OrderActor — SLA timeout via Akka Timers
Behaviors.withTimers { timers =>
  timers.startSingleTimer(
    SlaTimeoutKey,
    SlaTimeout(orderId),
    15.minutes
  )
  // timer cancels itself on ConfirmOrder or CancelOrder
}
```

**Pending order API**:
```
GET /api/orders?status=PENDING&customerId={id}   → list customer's pending orders
GET /api/orders?status=PENDING&ageMinutes=15      → ops view of stale orders
```

---

### Scenario 2: CONFIRMED → FULFILLED (Completed Orders)

**What happens on fulfillment**:

```
ShipmentService / external webhook
  → POST /api/orders/{id}/fulfill   (internal only, not exposed via gateway)
  → OrderActor receives FulfillOrder command
  → CONFIRMED → FULFILLED
  → persist OrderFulfilled event
  → publish order.fulfilled to Kafka

  Inventory Service consumes order.fulfilled:
    → UPDATE inventory_reservations SET status='COMMITTED' WHERE order_id = $id
    → (reservation becomes a real deduction)

  Notification Service consumes order.fulfilled:
    → send "Your order has been shipped" email/SMS/push
    → include tracking number
```

**Data retention for FULFILLED orders**:

```
┌────────────────────────────────────────────────────────────────┐
│  Completed Order Data Policy                                   │
│                                                                │
│  Active DB (PostgreSQL):                                       │
│    • Keep FULFILLED orders for 90 days (query / return window) │
│    • index on (customer_id, status, created_at DESC)           │
│                                                                │
│  Archive (cold storage / S3 Parquet):                          │
│    • After 90 days → archive job moves to S3                   │
│    • Akka Persistence journal events kept indefinitely          │
│      (event sourcing = immutable audit log, never delete)      │
│                                                                │
│  Redis cache:                                                  │
│    • Do NOT cache FULFILLED order details in Redis             │
│    • Reads are infrequent; serve from Postgres directly        │
└────────────────────────────────────────────────────────────────┘
```

**Completed order query patterns**:
```
GET /api/orders/{id}                              → single order (all statuses)
GET /api/orders?customerId={id}&status=FULFILLED  → customer order history
GET /api/orders?customerId={id}&from=2026-01-01   → date-range query (uses index)
```

---

### Scenario 3: CANCELLED Orders — Inventory Release + Notification

**Two cancel paths**:

```
Path A: Customer-initiated cancel (PENDING or CONFIRMED, before shipment)
  Client → DELETE /api/orders/{id}
         → Gateway → Order Service
         → CancelOrder(id, reason="CUSTOMER_REQUEST")
         → OrderActor: PENDING|CONFIRMED → CANCELLED
         → publish order.cancel.requested

Path B: Saga compensation (CONFIRMED, inventory rejected)
  Inventory Service → publishes order.cancel.requested
  → Order Service consumes → CancelOrder(id, reason="INSUFFICIENT_STOCK")
  → PENDING → CANCELLED
```

**What each service does on order.cancelled event**:

```
┌─────────────────────────────────────────────────────────────────┐
│  Inventory Service (consumes order.cancelled)                   │
│                                                                 │
│  IF order was CONFIRMED (stock was reserved):                   │
│    UPDATE inventory                                             │
│    SET reserved_qty = reserved_qty - cancelled_qty              │
│    WHERE product_id IN (cancelled order items)                  │
│    UPDATE inventory_reservations SET status='RELEASED'          │
│    DEL redis:inventory:{productId}  (invalidate cache)          │
│    publish inventory.updated (status=RELEASED)                  │
│                                                                 │
│  IF order was PENDING (stock was never reserved):               │
│    → no-op, just ack the Kafka message                          │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  Notification Service (consumes order.cancelled)                │
│                                                                 │
│  reason="CUSTOMER_REQUEST"  → "Your order has been cancelled"   │
│  reason="INSUFFICIENT_STOCK"→ "Sorry, item out of stock"        │
│  reason="SLA_TIMEOUT"       → "Your order expired — try again"  │
│  reason="PAYMENT_FAILED"    → "Payment could not be processed"  │
│                                                                 │
│  Channel: email (always) + SMS (if phone on record)             │
└─────────────────────────────────────────────────────────────────┘
```

**New Kafka event — `order.cancelled`**:
```json
{
  "eventType": "ORDER_CANCELLED",
  "payload": {
    "orderId":    "ord-7f3a...",
    "customerId": "3fa85f64...",
    "reason":     "CUSTOMER_REQUEST",
    "cancelledAt": "2026-06-29T11:00:00Z",
    "wasConfirmed": true,
    "items": [{ "productId": "prod-001", "quantity": 2 }]
  }
}
```

**New commands and events added to Order Service**:
```scala
// New commands
case class RequestReturn(orderId: String, reason: String,
                         items: List[ItemLine],
                         replyTo: ActorRef[OrderReply]) extends OrderCommand
case class ApproveReturn(orderId: String, replyTo: ActorRef[OrderReply]) extends OrderCommand
case class RejectReturn(orderId: String, reason: String,
                        replyTo: ActorRef[OrderReply]) extends OrderCommand

// New events
case class ReturnRequested(orderId: String, reason: String,
                           items: List[ItemLine], at: Instant) extends OrderEvent
case class ReturnApproved(orderId: String, at: Instant)  extends OrderEvent
case class ReturnRejected(orderId: String, reason: String, at: Instant) extends OrderEvent

// New status in OrderStatus
case object ReturnRequested extends OrderStatus
case object Returned        extends OrderStatus
```

---

### Scenario 4: RETURNED Orders — Return Flow + Refund Trigger

**Return eligibility rules (enforced in Service layer)**:

```scala
def requestReturn(orderId: String, reason: String): Future[Either[ServiceError, OrderResponse]] = {
  orderRepo.findById(orderId).flatMap {
    case None                            => Future.successful(Left(OrderNotFound))
    case Some(o) if o.status != Fulfilled => Future.successful(Left(InvalidTransition("only FULFILLED orders can be returned")))
    case Some(o) if isOutsideReturnWindow(o.createdAt) => Future.successful(Left(ReturnWindowExpired("30-day return window exceeded")))
    case Some(o) => // proceed: send RequestReturn to actor
  }
}

private def isOutsideReturnWindow(createdAt: Instant): Boolean =
  createdAt.isBefore(Instant.now().minus(30, ChronoUnit.DAYS))
```

**Full return flow**:

```
Customer → POST /api/orders/{id}/return  { reason, items }
         ↓
  Order Service:
    1. validate: status=FULFILLED, within 30-day window
    2. send RequestReturn to actor → FULFILLED → RETURN_REQUESTED
    3. persist ReturnRequested event
    4. publish order.return.requested to Kafka
         ↓
  [Auto-approve OR ops review]:
    5. PUT /api/orders/{id}/return/approve  (internal ops API)
       → actor: RETURN_REQUESTED → RETURNED
       → persist ReturnApproved event
       → publish order.returned to Kafka
         ↓
  Inventory Service (consumes order.returned):
    6. UPDATE inventory SET quantity = quantity + returned_qty
       (returned items go back into stock)
    7. DEL redis:inventory:{productId}
    8. publish inventory.updated (status=RESTOCKED)
         ↓
  Payment Service / Refund trigger (new integration point):
    9. publish refund.requested event (for future payment service)
       OR call RefundClient.initiateRefund(orderId, amount) directly
         ↓
  Notification Service (consumes order.returned):
   10. send "Return approved — refund of $X initiated" email + SMS
```

**New Kafka topics for return flow**:

```
order.return.requested    — 1 partition, RF=1   (low volume)
order.returned            — 1 partition, RF=1
refund.requested          — 1 partition, RF=2   (financial — higher durability)
```

**Return API endpoints** (add to openapi.yaml):
```
POST /api/orders/{id}/return           → request return (customer)
PUT  /api/orders/{id}/return/approve   → approve return (internal ops)
PUT  /api/orders/{id}/return/reject    → reject return  (internal ops)
GET  /api/orders/{id}/return           → get return status
```

---

### Updated DB Schema — Orders Table (extended)

```sql
ALTER TABLE orders ADD COLUMN cancelled_reason TEXT;
ALTER TABLE orders ADD COLUMN cancelled_at      TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN return_requested_at TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN return_reason     TEXT;
ALTER TABLE orders ADD COLUMN returned_at       TIMESTAMPTZ;
ALTER TABLE orders ADD COLUMN refund_amount     NUMERIC(12,2);

-- New status CHECK constraint (replace old one)
ALTER TABLE orders DROP CONSTRAINT IF EXISTS orders_status_check;
ALTER TABLE orders ADD CONSTRAINT orders_status_check
  CHECK (status IN ('PENDING','CONFIRMED','CANCELLED','FULFILLED',
                    'RETURN_REQUESTED','RETURNED'));

-- New index for return queries
CREATE INDEX idx_orders_return_status ON orders(status)
  WHERE status IN ('RETURN_REQUESTED');

-- Background job: stale pending orders
CREATE INDEX idx_orders_stale_pending ON orders(created_at)
  WHERE status = 'PENDING';
```

---

### Background Jobs Summary

```
┌──────────────────────────────────────────────────────────────────────┐
│  Job                     │ Trigger   │ Action                        │
├──────────────────────────┼───────────┼───────────────────────────────┤
│ PendingOrderSlaJob       │ every 1m  │ Cancel PENDING orders > 15m   │
│ OrderArchiveJob          │ daily 2am │ Archive FULFILLED > 90d to S3 │
│ StaleReturnJob           │ daily 9am │ Alert ops on RETURN_REQUESTED  │
│                          │           │ > 3 days with no action        │
│ InventoryReconcileJob    │ hourly    │ Assert reserved_qty = sum of   │
│                          │           │ ACTIVE reservations in DB      │
└──────────────────────────┴───────────┴───────────────────────────────┘
```

All jobs implemented as Akka Streams `Source.tick(...)` pipelines in the respective service — not as cron jobs or separate processes. This keeps them part of the service lifecycle (start/stop with the pod).

---

### Summary Table — What Happens in Each State

| State | Inventory | Notification | DB | Kafka Event |
|-------|-----------|-------------|-----|------------|
| **PENDING** | No lock held | "Order received" | row inserted | `order.created` |
| **CONFIRMED** | Stock reserved | "Order confirmed" | status updated | `order.confirmed` |
| **FULFILLED** | Reservation committed (deducted) | "Order shipped" | status + timestamps | `order.fulfilled` |
| **CANCELLED** (from PENDING) | No-op | "Order cancelled" + reason | status + cancelled_at + reason | `order.cancelled` |
| **CANCELLED** (from CONFIRMED) | Reservation released | "Order cancelled" + reason | same | `order.cancelled` (wasConfirmed=true) |
| **RETURN_REQUESTED** | No change | "Return request received" | return_requested_at | `order.return.requested` |
| **RETURNED** | Items restocked | "Refund of $X initiated" | returned_at + refund_amount | `order.returned` + `refund.requested` |

---

# 3. Implementation Plan <a name="impl"></a>

> Phases 2, 3, 4 are fully independent and can be built in parallel after Phase 0.

---

## Critical Structural Issues to Fix First

> ⚠️ Fix these before writing any service code — they will cause build failures.

| # | Issue | Fix |
|---|-------|-----|
| 1 | Root has both `build.sbt` AND `pom.xml` — ambiguous dual build system | Use `build.sbt` for Scala modules only (`order-service`, `inventory-service`, `shared`); `pom.xml` as Maven parent for Java modules (`api-gateway`, `notification-service`) |
| 2 | Root `src/main/scala/` and `src/main/java/` conflict with service subdirs (`order-service/`, `api-gateway/`) | Move all source into `<service-name>/src/...`; one module = one `src/` tree |

---

## Phase 0 — Foundation *(Build First — all phases depend on this)*

### 0.1 — Multi-Module Project Structure

**Root repository layout** — one repo, all 5 modules:

```
order-processing-system/                  ← git root
│
├── build.sbt                             ← SBT multi-project (Scala modules)
├── project/
│   ├── build.properties                  # sbt.version=1.10.0
│   └── plugins.sbt                       # sbt-assembly, flyway-sbt
│
├── pom.xml                               ← Maven parent (Java modules)
│
├── .env.example                          ← local dev env vars template (committed)
├── .env                                  ← actual secrets (gitignored)
├── docker-compose.yml                    ← full local dev stack
├── docker-compose.override.yml           ← dev hot-reload overrides (gitignored)
│
├── shared/                               ← Scala module — Kafka event schemas
│   ├── build.sbt                         # (overridden by root build.sbt)
│   └── src/main/scala/com/ops/shared/
│
├── order-service/                        ← Scala module
│   ├── Dockerfile
│   └── src/
│       ├── main/scala/com/ops/order/
│       └── main/resources/db/migration/
│
├── inventory-service/                    ← Scala module
│   ├── Dockerfile
│   └── src/
│       ├── main/scala/com/ops/inventory/
│       └── main/resources/db/migration/
│
├── api-gateway/                          ← Java/Maven module
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/main/java/com/ops/gateway/
│
├── notification-service/                 ← Java/Maven module
│   ├── pom.xml
│   ├── Dockerfile
│   └── src/
│       ├── main/java/com/ops/notification/
│       └── main/resources/db/migration/
│
└── k8s/                                  ← Kubernetes manifests (no build)
    ├── namespace.yaml
    ├── configmaps/
    ├── secrets/
    ├── deployments/
    ├── services/
    ├── ingress/
    └── hpa/
```

**Root `build.sbt`** (SBT multi-project — complete):
```scala
ThisBuild / scalaVersion := "3.4.2"
ThisBuild / organization := "com.ops"

val akkaVersion         = "2.9.3"
val akkaHttpVersion     = "10.6.3"
val alpakkaKafkaVersion = "5.0.0"
val slickVersion        = "3.5.1"
val circeVersion        = "0.14.9"
val flywayVersion       = "10.15.0"

// ── shared: Kafka event schemas (no framework deps) ──────────────────
lazy val shared = (project in file("shared"))
  .settings(
    name := "shared",
    libraryDependencies ++= Seq(
      "io.circe" %% "circe-core"    % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser"  % circeVersion
    )
  )

// ── order-service: Akka Typed + Persistence + Akka HTTP ──────────────
lazy val orderService = (project in file("order-service"))
  .dependsOn(shared)
  .settings(
    name := "order-service",
    libraryDependencies ++= Seq(
      "com.typesafe.akka"  %% "akka-actor-typed"          % akkaVersion,
      "com.typesafe.akka"  %% "akka-persistence-typed"    % akkaVersion,
      "com.typesafe.akka"  %% "akka-http"                 % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-stream-kafka"         % alpakkaKafkaVersion,
      "com.lightbend.akka" %% "akka-persistence-jdbc"     % "5.4.1",
      "com.typesafe.slick" %% "slick"                     % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"            % slickVersion,
      "org.postgresql"      % "postgresql"                 % "42.7.3",
      "org.flywaydb"        % "flyway-core"                % flywayVersion,
      "org.flywaydb"        % "flyway-database-postgresql" % flywayVersion,
      "com.typesafe.akka"  %% "akka-actor-testkit-typed"  % akkaVersion  % Test,
      "org.scalatest"      %% "scalatest"                 % "3.2.19"     % Test
    ),
    assembly / mainClass := Some("com.ops.order.OrderServiceApp"),
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", _*) => MergeStrategy.discard
      case _                        => MergeStrategy.first
    }
  )

// ── inventory-service: Akka Streams + Alpakka Kafka ──────────────────
lazy val inventoryService = (project in file("inventory-service"))
  .dependsOn(shared)
  .settings(
    name := "inventory-service",
    libraryDependencies ++= Seq(
      "com.typesafe.akka"  %% "akka-stream"               % akkaVersion,
      "com.typesafe.akka"  %% "akka-http"                 % akkaHttpVersion,
      "com.typesafe.akka"  %% "akka-stream-kafka"         % alpakkaKafkaVersion,
      "com.typesafe.slick" %% "slick"                     % slickVersion,
      "com.typesafe.slick" %% "slick-hikaricp"            % slickVersion,
      "io.lettuce"          % "lettuce-core"              % "6.3.2.RELEASE",
      "org.postgresql"      % "postgresql"                 % "42.7.3",
      "org.flywaydb"        % "flyway-core"                % flywayVersion,
      "org.flywaydb"        % "flyway-database-postgresql" % flywayVersion,
      "com.typesafe.akka"  %% "akka-stream-testkit"       % akkaVersion  % Test,
      "org.scalatest"      %% "scalatest"                 % "3.2.19"     % Test
    ),
    assembly / mainClass := Some("com.ops.inventory.InventoryApp")
  )

// ── root: aggregate — run sbt test / sbt assembly from root ──────────
lazy val root = (project in file("."))
  .aggregate(shared, orderService, inventoryService)
  .settings(name := "order-processing-system")
```

**Root `pom.xml`** (Maven parent — complete):
```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0
         https://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>com.ops</groupId>
  <artifactId>order-processing-parent</artifactId>
  <version>1.0.0-SNAPSHOT</version>
  <packaging>pom</packaging>

  <parent>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-parent</artifactId>
    <version>3.3.1</version>
  </parent>

  <properties>
    <java.version>21</java.version>
    <spring-cloud.version>2023.0.2</spring-cloud.version>
  </properties>

  <modules>
    <module>api-gateway</module>
    <module>notification-service</module>
  </modules>

  <dependencyManagement>
    <dependencies>
      <dependency>
        <groupId>org.springframework.cloud</groupId>
        <artifactId>spring-cloud-dependencies</artifactId>
        <version>${spring-cloud.version}</version>
        <type>pom</type>
        <scope>import</scope>
      </dependency>
    </dependencies>
  </dependencyManagement>

  <build>
    <plugins>
      <plugin>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-maven-plugin</artifactId>
      </plugin>
    </plugins>
  </build>
</project>
```

**Build commands**:
```bash
# Build all Scala modules from root
sbt clean assembly

# Build all Java modules from root
mvn clean package -DskipTests

# Build single module
sbt "orderService/assembly"
mvn -pl notification-service package

# Run all tests
sbt test
mvn test
```

### 0.2 — Shared domain events module (`shared/`)

All Kafka event contracts live here. All other services import this module.

```
shared/src/main/scala/com/ops/shared/
  events/
    OrderCreatedEvent.scala       # case class with orderId, customerId, items, total
    InventoryUpdatedEvent.scala   # case class with productId, quantity, status
    NotificationSentEvent.scala   # case class with notifId, channel, recipient
  serialization/
    EventJsonFormats.scala        # circe encoders/decoders
  domain/
    Money.scala                   # value object
    ItemLine.scala                # value object
```

Serialization: circe (Scala) / Jackson (Java). JSON schema must be backward-compatible.

### 0.3 — Root `docker-compose.yml` and `.env`

**`.env.example`** (committed to git — template only, no real secrets):
```dotenv
# ── PostgreSQL ──────────────────────────────────────────
POSTGRES_USER=ops
POSTGRES_PASSWORD=changeme
POSTGRES_HOST=postgres
POSTGRES_PORT=5432

# ── Redis ───────────────────────────────────────────────
REDIS_HOST=redis
REDIS_PORT=6379

# ── Kafka ───────────────────────────────────────────────
KAFKA_BOOTSTRAP=kafka:9092

# ── JWT (RS256 public key path or inline) ───────────────
JWT_PUBLIC_KEY=classpath:keys/public.pem

# ── Service ports ───────────────────────────────────────
GATEWAY_PORT=8080
ORDER_PORT=8081
INVENTORY_PORT=8082
NOTIFICATION_PORT=8083
```

Copy to `.env` (gitignored) and fill real values before running locally.

### 5.4 — Complete `docker-compose.yml`

Three separate PostgreSQL databases (database-per-service pattern), full healthchecks on every infra container, all env vars wired from `.env`, and `depends_on: condition: service_healthy` so services wait until infra is truly ready — not just started.

```yaml
# docker-compose.yml
# Run locally: docker-compose up --build
# Stop + clean: docker-compose down -v

name: order-processing-system

# ── re-usable env block for all service containers ──────────────────
x-service-env: &service-env
  KAFKA_BOOTSTRAP: ${KAFKA_BOOTSTRAP:-kafka:9092}
  REDIS_HOST:      ${REDIS_HOST:-redis}
  REDIS_PORT:      ${REDIS_PORT:-6379}
  JWT_PUBLIC_KEY:  ${JWT_PUBLIC_KEY:-classpath:keys/public.pem}

services:

  # ── Infrastructure ──────────────────────────────────────────────────

  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    environment:
      POSTGRES_USER:     ${POSTGRES_USER:-ops}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
      # Init script creates 3 separate DBs (one per service)
      POSTGRES_MULTIPLE_DATABASES: ops_orders,ops_inventory,ops_notifications
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./infra/postgres/init-multiple-dbs.sh:/docker-entrypoint-initdb.d/init-dbs.sh:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER:-ops} -d ops_orders"]
      interval: 5s
      timeout: 5s
      retries: 10
      start_period: 10s

  redis:
    image: redis:7-alpine
    restart: unless-stopped
    command: >
      redis-server
      --maxmemory 256mb
      --maxmemory-policy allkeys-lru
      --save 60 1
      --loglevel warning
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    healthcheck:
      test: ["CMD", "redis-cli", "ping"]
      interval: 5s
      timeout: 3s
      retries: 5

  kafka:
    image: confluentinc/cp-kafka:7.6.1
    restart: unless-stopped
    environment:
      # KRaft mode — no Zookeeper
      KAFKA_NODE_ID:                        1
      KAFKA_PROCESS_ROLES:                  broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS:       1@kafka:9093
      KAFKA_LISTENERS:                      PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS:           PLAINTEXT://kafka:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES:      CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME:     PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR:  1
      KAFKA_LOG_RETENTION_HOURS:            168     # 7 days
      KAFKA_AUTO_CREATE_TOPICS_ENABLE:      "false" # explicit topic creation only
      # Required: unique cluster ID for KRaft (generate once: kafka-storage random-uuid)
      CLUSTER_ID:                           "MkU3OEVBNTcwNTJENDM2Qk"
    ports:
      - "9092:9092"
    volumes:
      - kafka_data:/var/lib/kafka/data
    healthcheck:
      test: ["CMD-SHELL",
             "kafka-broker-api-versions --bootstrap-server localhost:9092 > /dev/null 2>&1"]
      interval: 10s
      timeout: 10s
      retries: 10
      start_period: 30s

  kafka-init:
    # One-shot container: creates topics then exits
    image: confluentinc/cp-kafka:7.6.1
    depends_on:
      kafka:
        condition: service_healthy
    command: >
      bash -c "
        echo 'Creating Kafka topics...'
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.created        --partitions 3 --replication-factor 1 \
          --config retention.ms=604800000 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.cancelled      --partitions 3 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.return.requested --partitions 1 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.returned       --partitions 1 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic inventory.updated    --partitions 3 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic notif.sent           --partitions 1 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.created.DLT    --partitions 1 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic order.cancel.requested --partitions 3 --replication-factor 1 &&
        kafka-topics --bootstrap-server kafka:9092 --create --if-not-exists \
          --topic refund.requested     --partitions 1 --replication-factor 1 &&
        echo 'All topics created.'
      "
    restart: "no"   # run once only

  kafka-ui:
    image: provectuslabs/kafka-ui:latest
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully   # topics must exist before UI starts
    ports:
      - "8090:8080"
    environment:
      # ── Cluster connection ──────────────────────────────────────────
      KAFKA_CLUSTERS_0_NAME:             local-ops
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: kafka:9092

      # ── Read-only mode: show topic messages + consumer group lag ────
      KAFKA_CLUSTERS_0_READONLY:         "false"   # set true in staging/prod

      # ── Consumer group lag polling (updates every 5s in UI) ─────────
      KAFKA_CLUSTERS_0_CONSUMER_GROUPS_FETCH_PERIOD_MILLIS: 5000

      # ── Default message deserialization: try JSON first ─────────────
      KAFKA_CLUSTERS_0_DEFAULTKEYSERDE:   String
      KAFKA_CLUSTERS_0_DEFAULTVALUESERDE: SchemaRegistry   # or "String" if no schema registry

      # ── Auth: protect the UI with basic auth (local dev) ────────────
      AUTH_TYPE: LOGIN_FORM
      SPRING_SECURITY_USER_NAME:     admin
      SPRING_SECURITY_USER_PASSWORD: admin

      # ── Useful topic display settings ────────────────────────────────
      DYNAMIC_CONFIG_ENABLED: "true"     # allows topic config changes from UI

  # ── Application Services ────────────────────────────────────────────

  api-gateway:
    build:
      context: ./api-gateway
      dockerfile: Dockerfile
    restart: unless-stopped
    depends_on:
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
    ports:
      - "${GATEWAY_PORT:-8080}:8080"
    environment:
      <<: *service-env
      SPRING_PROFILES_ACTIVE: local
      SERVER_PORT: 8080
      ORDER_SERVICE_URL:        http://order-service:8081
      INVENTORY_SERVICE_URL:    http://inventory-service:8082
      NOTIFICATION_SERVICE_URL: http://notification-service:8083
      SPRING_REDIS_HOST:        ${REDIS_HOST:-redis}
      SPRING_REDIS_PORT:        ${REDIS_PORT:-6379}
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8080/actuator/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 30s

  order-service:
    build:
      context: ./order-service
      dockerfile: Dockerfile
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    ports:
      - "${ORDER_PORT:-8081}:8081"
    environment:
      <<: *service-env
      DB_URL:      jdbc:postgresql://${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/ops_orders
      DB_USER:     ${POSTGRES_USER:-ops}
      DB_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
      HTTP_PORT:   8081
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8081/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 40s   # Flyway migrations + ActorSystem boot

  inventory-service:
    build:
      context: ./inventory-service
      dockerfile: Dockerfile
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
      redis:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    ports:
      - "${INVENTORY_PORT:-8082}:8082"
    environment:
      <<: *service-env
      DB_URL:      jdbc:postgresql://${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/ops_inventory
      DB_USER:     ${POSTGRES_USER:-ops}
      DB_PASSWORD: ${POSTGRES_PASSWORD:-changeme}
      HTTP_PORT:   8082
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8082/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 40s

  notification-service:
    build:
      context: ./notification-service
      dockerfile: Dockerfile
    restart: unless-stopped
    depends_on:
      postgres:
        condition: service_healthy
      kafka:
        condition: service_healthy
      kafka-init:
        condition: service_completed_successfully
    ports:
      - "${NOTIFICATION_PORT:-8083}:8083"
    environment:
      <<: *service-env
      SPRING_PROFILES_ACTIVE:        local
      SERVER_PORT:                   8083
      SPRING_DATASOURCE_URL:         jdbc:postgresql://${POSTGRES_HOST:-postgres}:${POSTGRES_PORT:-5432}/ops_notifications
      SPRING_DATASOURCE_USERNAME:    ${POSTGRES_USER:-ops}
      SPRING_DATASOURCE_PASSWORD:    ${POSTGRES_PASSWORD:-changeme}
      SPRING_KAFKA_BOOTSTRAP_SERVERS: ${KAFKA_BOOTSTRAP:-kafka:9092}
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:8083/actuator/health || exit 1"]
      interval: 15s
      timeout: 5s
      retries: 5
      start_period: 30s

# ── Volumes ───────────────────────────────────────────────────────────
volumes:
  postgres_data:
  redis_data:
  kafka_data:

# ── Network (explicit — all services on same bridge) ─────────────────
networks:
  default:
    name: ops-network
    driver: bridge
```

---

### Postgres Init Script — 3 Databases (`infra/postgres/init-multiple-dbs.sh`)

Docker's official Postgres image runs any `.sh` in `/docker-entrypoint-initdb.d/` on first boot. This creates one database per service:

```bash
#!/bin/bash
# infra/postgres/init-multiple-dbs.sh
# Creates separate databases for each service (database-per-service pattern)
set -e

create_db() {
  local db=$1
  echo "Creating database: $db"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    CREATE DATABASE $db;
    GRANT ALL PRIVILEGES ON DATABASE $db TO $POSTGRES_USER;
EOSQL
}

# Parse comma-separated list from POSTGRES_MULTIPLE_DATABASES env var
if [ -n "$POSTGRES_MULTIPLE_DATABASES" ]; then
  for db in $(echo "$POSTGRES_MULTIPLE_DATABASES" | tr ',' ' '); do
    create_db "$db"
  done
fi

echo "Databases created: $POSTGRES_MULTIPLE_DATABASES"
```

**Result**: Three isolated databases:
- `ops_orders` → owned by Order Service (Flyway runs V1–V4 on startup)
- `ops_inventory` → owned by Inventory Service (Flyway runs V1–V2 on startup)
- `ops_notifications` → owned by Notification Service (Flyway runs V1–V2 on startup)

---

### Quick Start — Local Dev

```bash
# 1. Clone and set up env
cp .env.example .env
# Edit .env with your local values (or leave defaults)

# 2. Build all service images
sbt clean assembly           # builds Scala fat JARs
mvn clean package -DskipTests # builds Java fat JARs

# 3. Start everything
docker-compose up --build

# 4. Verify
curl http://localhost:8080/actuator/health   # Gateway
curl http://localhost:8081/health            # Order Service
curl http://localhost:8082/health            # Inventory Service
curl http://localhost:8083/actuator/health   # Notification Service
open http://localhost:8090                   # Kafka UI
open http://localhost:8080/swagger-ui.html   # Swagger

# 5. Tear down (keep volumes)
docker-compose down

# 6. Tear down + wipe all data
docker-compose down -v
```

---

### `docker-compose.override.yml` — Hot Reload for Local Dev (gitignored)

For development, mount source and skip the fat JAR build:

```yaml
# docker-compose.override.yml  (gitignored — developer-local only)
services:
  order-service:
    build:
      target: development     # stop at builder stage, don't copy fat JAR
    volumes:
      - ./order-service:/app  # mount source for sbt ~run hot reload
    command: ["sbt", "~reStart"]

  notification-service:
    volumes:
      - ./notification-service:/app
    command: ["./mvnw", "spring-boot:run", "-Dspring-boot.run.jvmArguments=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"]
    ports:
      - "5005:5005"   # remote debug port
```

---

## Phase 1 — API Gateway (`api-gateway/`)

### 1.1 — Maven project setup

```xml
Dependencies:
  spring-boot-starter-webflux
  spring-cloud-starter-gateway
  springdoc-openapi-webflux-ui
  spring-boot-starter-actuator
  resilience4j-spring-boot3
  jjwt (JWT validation)
```

### 1.2 — Source structure

```
api-gateway/src/main/java/com/ops/gateway/
  GatewayApplication.java          # @SpringBootApplication entry point
  config/
    RouteConfig.java               # Spring Cloud Gateway route definitions
    SecurityConfig.java            # WebFlux security, JWT setup
  filter/
    JwtAuthFilter.java             # Global filter — validate JWT before routing
    RequestLoggingFilter.java      # Structured request/response logging
    RateLimitFilter.java           # Redis-based rate limiting per client
  fallback/
    FallbackController.java        # Circuit breaker fallback responses
```

### 1.3 — `application.yml` route table

```yaml
spring:
  cloud:
    gateway:
      routes:
        - id: order-service
          uri: http://order-service:8081
          predicates: [Path=/api/orders/**]
          filters: [CircuitBreaker=orderCB, StripPrefix=1]
        - id: inventory-service
          uri: http://inventory-service:8082
          predicates: [Path=/api/inventory/**]
          filters: [CircuitBreaker=inventoryCB, StripPrefix=1]
        - id: notification-service
          uri: http://notification-service:8083
          predicates: [Path=/api/notifications/**]
          filters: [CircuitBreaker=notifCB, StripPrefix=1]
```

### 1.4 — `openapi.yaml` (contract-first)

Define all endpoints before implementing downstream services:
- `POST /api/orders` — create order
- `GET /api/orders/{id}` — get order by ID
- `GET /api/orders` — list orders (paginated)
- `GET /api/inventory/{productId}` — get inventory level
- `PUT /api/inventory/{productId}` — update inventory
- `POST /api/inventory/reserve` — reserve stock
- `GET /api/notifications` — list notifications

Swagger UI served at `/swagger-ui.html`.

---

## Phase 2 — Order Service (`order-service/`) *(parallel with Phase 3 & 4)*

### 2.1 — SBT project setup

```scala
libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-actor-typed"         % akkaVersion,
  "com.typesafe.akka"  %% "akka-persistence-typed"   % akkaVersion,  // event sourcing
  "com.typesafe.akka"  %% "akka-http"                % akkaHttpVersion,
  "com.typesafe.akka"  %% "akka-stream-kafka"        % alpakkaKafkaVersion,
  "com.typesafe.slick" %% "slick"                    % slickVersion,
  "io.circe"           %% "circe-generic"             % circeVersion,
  "org.flywaydb"        % "flyway-core"               % "10.15.0",
  "org.flywaydb"        % "flyway-database-postgresql" % "10.15.0"
)
```

### 2.2 — Source structure

```
order-service/src/main/scala/com/ops/order/
  OrderServiceApp.scala            # ActorSystem[Nothing] bootstrap + HTTP bind
  ── ENDPOINT LAYER ──────────────────────────────────────────────
  api/
    OrderController.scala          # Akka HTTP routes: unmarshal → validate → call service
    dto/
      CreateOrderRequest.scala     # Input DTO (validated fields)
      OrderResponse.scala          # Output DTO (client-facing)
      OrderListResponse.scala      # Paginated wrapper
  ── SERVICE LAYER ───────────────────────────────────────────────
  service/
    OrderService.scala             # trait: createOrder, getOrder, listOrders, cancelOrder
    OrderServiceImpl.scala         # impl: business rules, actor ask, Kafka publish
  actor/
    OrderActor.scala               # Akka Typed EventSourcedBehavior (state machine)
    OrderSupervisor.scala          # Spawns/routes to per-order actors
  ── REPOSITORY LAYER ────────────────────────────────────────────
  repository/
    OrderRepository.scala          # trait: save, findById, findByCustomer, updateStatus
    OrderRepositoryImpl.scala      # Slick async impl (HikariCP pool max=20)
    OrderSchema.scala              # Slick table mapping
  ── INFRASTRUCTURE (cross-cutting) ──────────────────────────────
  kafka/
    OrderEventProducer.scala       # Alpakka Kafka: called from service layer only
  domain/
    Order.scala                    # pure case class — no framework deps
    OrderStatus.scala              # sealed trait
    OrderCommand.scala             # sealed trait
    OrderEvent.scala               # sealed trait
  infra/
    DatabaseConfig.scala           # HikariCP + Slick config
  ── MIGRATIONS ──────────────────────────────────────────────────────
  src/main/resources/db/migration/
    V1__create_orders_table.sql
    V2__create_akka_journal_tables.sql
    V3__create_akka_snapshot_table.sql
    V4__add_return_columns.sql
```

### 2.3 — Event sourcing (key design)

```scala
// OrderActor uses EventSourcedBehavior:
// Command  → validate → persist Event → update State → reply
// On restart: replay persisted events to rebuild state (full audit trail)

EventSourcedBehavior[OrderCommand, OrderEvent, OrderState](
  persistenceId = PersistenceId.ofUniqueId(s"order-${orderId}"),
  emptyState    = OrderState.empty,
  commandHandler = (state, command) => ...,
  eventHandler   = (state, event)   => ...
)
```

### 2.4 — Kafka flow

```
OrderActor persists OrderCreated event
  → OrderEventProducer publishes OrderCreatedEvent to Kafka topic "order.created"
  → Inventory Service consumes and reserves stock
  → Notification Service consumes and sends confirmation
```

### 2.5 — PostgreSQL schema

> Full DDL with all constraints, indexes, Akka journal/snapshot tables, and reservations table is in **LLD Section 2.3**.

Quick reference:
```sql
CREATE TABLE orders (
  id UUID PRIMARY KEY, customer_id UUID, status VARCHAR(20),
  items JSONB, total_amount NUMERIC(12,2), idempotency_key TEXT UNIQUE,
  created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ
);
```

---

## Phase 3 — Inventory Service (`inventory-service/`) *(parallel with Phase 2 & 4)*

### 3.1 — SBT project setup

```scala
libraryDependencies ++= Seq(
  "com.typesafe.akka"  %% "akka-stream"              % akkaVersion,
  "com.typesafe.akka"  %% "akka-stream-kafka"        % alpakkaKafkaVersion,
  "com.typesafe.akka"  %% "akka-http"                % akkaHttpVersion,
  "com.typesafe.slick" %% "slick"                    % slickVersion,
  "io.lettuce"          % "lettuce-core"             % lettuceVersion,
  "io.circe"           %% "circe-generic"             % circeVersion,
  "org.flywaydb"        % "flyway-core"               % "10.15.0",
  "org.flywaydb"        % "flyway-database-postgresql" % "10.15.0"
)
```

### 3.2 — Source structure

```
inventory-service/src/main/scala/com/ops/inventory/
  InventoryApp.scala               # ActorSystem + Akka Streams bootstrap
  ── ENDPOINT LAYER ──────────────────────────────────────────────
  api/
    InventoryController.scala      # Akka HTTP routes: unmarshal → validate → call service
    dto/
      InventoryResponse.scala
      ReserveRequest.scala
      UpdateStockRequest.scala
  ── SERVICE LAYER ───────────────────────────────────────────────
  service/
    InventoryService.scala         # trait: getItem, reserve, release, updateStock
    InventoryServiceImpl.scala     # business rules, optimistic-lock retry, Kafka publish
  stream/
    InventoryStream.scala          # Akka Streams Kafka consumer → calls InventoryService
  ── REPOSITORY LAYER ────────────────────────────────────────────
  repository/
    InventoryRepository.scala      # trait: findById, reserveWithLock, releaseReservation
    InventoryRepositoryImpl.scala  # Slick async + optimistic locking
    InventoryCache.scala           # Redis cache-aside wrapping InventoryRepository
    InventorySchema.scala          # Slick table mapping
  ── INFRASTRUCTURE ──────────────────────────────────────────────
  kafka/
    InventoryEventProducer.scala   # called from service layer only
  domain/
    InventoryItem.scala
    ReservationResult.scala        # Success(remaining) | Failure(reason)
  infra/
    RedisConfig.scala              # Lettuce async connection factory
    DatabaseConfig.scala
  ── MIGRATIONS ──────────────────────────────────────────────────────
  src/main/resources/db/migration/
    V1__create_inventory_tables.sql
    V2__add_sku_index.sql
```

### 3.3 — Akka Streams pipeline (core design)

```scala
// Backpressure-aware reactive pipeline:
Consumer.committableSource(consumerSettings, Subscriptions.topics("order.created"))
  .mapAsync(parallelism = 4) { msg =>
    deserialize(msg.record.value())           // OrderCreatedEvent
      .flatMap(event => reserveStock(event))  // DB update + cache invalidation
      .flatMap(result => publishUpdate(result)) // emit InventoryUpdatedEvent
      .map(_ => msg.committableOffset)
  }
  .via(Committer.flow(committerSettings))     // commit Kafka offset on success
  .run()
```

### 3.4 — Redis cache strategy

```
Read:  GET redis:inventory:{productId}
         HIT  → return cached value
         MISS → SELECT from postgres → SET redis:inventory:{productId} EX 300 (5 min TTL)
Write: UPDATE postgres inventory SET quantity = quantity - reserved WHERE version = $v
       DEL redis:inventory:{productId}  (invalidate on write)
```

Eviction policy: `allkeys-lru`. TTL: 300s. Stale inventory causes overselling — never skip invalidation.

### 3.5 — PostgreSQL schema

> Full DDL with optimistic locking, `inventory_reservations` join table, and constraints is in **LLD Section 2.3**.

Quick reference:
```sql
CREATE TABLE inventory (
  product_id UUID PRIMARY KEY, sku TEXT UNIQUE,
  quantity INT, reserved_qty INT, version BIGINT, updated_at TIMESTAMPTZ
);
CREATE TABLE inventory_reservations (
  id UUID PRIMARY KEY, order_id UUID, product_id UUID,
  quantity INT, status VARCHAR(10), created_at TIMESTAMPTZ
);
```

---

## Phase 4 — Notification Service (`notification-service/`) *(parallel with Phase 2 & 3)*

### 4.1 — Maven project setup

```xml
Dependencies:
  spring-boot-starter
  spring-kafka
  akka-actor-typed (Java API)
  spring-boot-starter-actuator
  spring-boot-starter-mail  (email channel)
```

### 4.2 — Source structure

```
notification-service/src/main/java/com/ops/notification/
  NotificationApp.java             # @SpringBootApplication entry
  ── ENDPOINT LAYER ──────────────────────────────────────────────
  controller/
    NotificationController.java    # GET /notifications, GET /notifications/{id}
  dto/
    NotificationResponse.java
    NotificationListResponse.java
  ── SERVICE LAYER ───────────────────────────────────────────────
  service/
    NotificationService.java       # interface: send*, getById, getByOrderId
    NotificationServiceImpl.java   # routes to actors, saves to DB, publishes event
    NotificationFactory.java       # builds channel-specific message payloads
  consumer/
    NotificationConsumer.java      # @KafkaListener → calls NotificationService ONLY
  actor/
    NotificationRouter.java        # Akka Typed: routes to channel actors
    EmailNotificationActor.java    # JavaMailSender integration
    SmsNotificationActor.java      # Twilio stub
  config/
    AkkaConfig.java                # @Configuration: ActorSystem bean
    KafkaConfig.java               # ConsumerFactory, container factory
  ── REPOSITORY LAYER ────────────────────────────────────────────
  repository/
    NotificationRepository.java    # Spring Data JPA interface (auto-impl)
  domain/
    NotificationRecord.java        # @Entity JPA mapping
  ── INFRASTRUCTURE ──────────────────────────────────────────────
  kafka/
    NotificationEventProducer.java # called from service layer only
  ── MIGRATIONS ──────────────────────────────────────────────────────
  src/main/resources/db/migration/
    V1__create_notifications_table.sql
    V2__add_notification_status_index.sql
```

### 4.3 — Dead Letter Topic (DLT) — required for resilience

```java
@RetryableTopic(
  attempts = "3",
  backoff = @Backoff(delay = 1000, multiplier = 2.0),
  dltStrategy = DltStrategy.FAIL_ON_ERROR
)
@KafkaListener(topics = "order.created")
public void onOrderCreated(OrderCreatedEvent event) { ... }
```

---

## Phase 5 — Infrastructure Configuration

### 5.1 — `application.conf` (shared Akka config)

```hocon
akka {
  actor {
    provider = "local"
    serialization-bindings {
      "com.ops.shared.events.OrderCreatedEvent" = jackson-json
    }
  }
  persistence {
    journal.plugin = "jdbc-journal"
    snapshot-store.plugin = "jdbc-snapshot-store"
  }
}
```

### 5.2 — Kafka topic definitions (`kafka/`)

```
Topics:
  order.created      — 3 partitions, RF=2, retention=7d
  inventory.updated  — 3 partitions, RF=2, retention=7d
  notif.sent         — 1 partition,  RF=1, retention=3d
  order.created.DLT  — 1 partition,  RF=1 (Dead Letter Topic)
```

### 5.3 — Multi-stage `Dockerfile` (per service)

```dockerfile
# Stage 1: Build
FROM eclipse-temurin:21-jdk AS builder
WORKDIR /app
COPY . .
RUN ./mvnw package -DskipTests   # or sbt assembly

# Stage 2: Runtime
FROM eclipse-temurin:21-jre
WORKDIR /app
COPY --from=builder /app/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
```

### 5.4 — `docker-compose.yml`

> The complete, production-grade `docker-compose.yml` with all health checks, env wiring, 3 separate databases, Kafka topic init container, and `.env` support is defined in **Phase 0 Section 0.3** above (moved there because it is a foundation dependency, not just infrastructure config).

Key properties of the final compose file:
- **9 services**: postgres, redis, kafka, kafka-init (one-shot), kafka-ui, api-gateway, order-service, inventory-service, notification-service
- **3 separate PostgreSQL databases**: `ops_orders`, `ops_inventory`, `ops_notifications` — created by init script on first boot
- **All `depends_on` use `condition: service_healthy`** — services never start before infra is ready
- **All topics created by `kafka-init`** one-shot container before any consumer starts
- **`.env` file** drives all secrets — nothing hardcoded
- **Named volumes** for postgres, redis, kafka — data survives `docker-compose down`
- **`docker-compose.override.yml`** for hot-reload dev mode (gitignored)

---

## Phase 6 — Kubernetes (`k8s/`)

### File structure

```
k8s/
  namespace.yaml                   # ops-system namespace
  configmaps/
    order-service-config.yaml
    inventory-service-config.yaml
    notification-service-config.yaml
    gateway-config.yaml
  secrets/
    db-credentials.yaml            # base64-encoded (or use external-secrets-operator)
    redis-auth.yaml
    kafka-sasl.yaml
  deployments/
    api-gateway-deployment.yaml
    order-service-deployment.yaml
    inventory-service-deployment.yaml
    notification-service-deployment.yaml
  services/
    api-gateway-service.yaml       # LoadBalancer (external traffic)
    order-service-service.yaml     # ClusterIP
    inventory-service-service.yaml # ClusterIP
    notification-service-service.yaml # ClusterIP
  ingress/
    gateway-ingress.yaml           # nginx Ingress → api-gateway
  hpa/
    order-service-hpa.yaml         # CPU 70% → scale 2-10 replicas
    inventory-service-hpa.yaml     # CPU 70% + Kafka consumer lag metric
```

### Deployment template (per service)

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: order-service
  namespace: ops-system
spec:
  replicas: 2
  strategy: { type: RollingUpdate }
  template:
    spec:
      containers:
        - name: order-service
          image: ops/order-service:latest
          resources:
            requests: { cpu: 250m, memory: 512Mi }
            limits:   { cpu: 1,    memory: 1Gi  }
          livenessProbe:
            httpGet: { path: /health, port: 8081 }
            initialDelaySeconds: 30
          readinessProbe:
            httpGet: { path: /health/ready, port: 8081 }
            initialDelaySeconds: 10
          envFrom:
            - configMapRef: { name: order-service-config }
            - secretRef:    { name: db-credentials }
```

---

## Phase 7 — Tests

### Unit tests

| Service | Framework | Key Tests |
|---------|-----------|-----------|
| Order Service | Akka TestKit (`BehaviorTestKit`) | Actor command/event/state transitions |
| Inventory Service | Akka Streams TestKit | Stream pipeline with mock Kafka source |
| Notification Service | JUnit 5 + Mockito | Consumer routing, actor dispatch |
| API Gateway | Spring `WebTestClient` | Route resolution, filter behavior, circuit breaker |

### Integration tests (Testcontainers)

```
test/
  OrderServiceIntegrationTest.scala   # Real Postgres + Kafka
  InventoryStreamIntegrationTest.scala # Real Kafka + Redis
  EndToEndFlowTest.java               # POST /orders → verify all 3 events in Kafka
```

End-to-end flow assertion:
```
POST /api/orders
  → Kafka: order.created ✓
  → Kafka: inventory.updated ✓  (within 5s)
  → Kafka: notif.sent ✓         (within 10s)
  → GET /api/orders/{id} returns status = CONFIRMED
```

---

## Implementation Order (Dependency Graph)

```
Phase 0 (Foundation: shared module + docker-compose infra)
    │
    ├─── Phase 1 (API Gateway)          [LLD §2.1 API contracts, §2.7 filter chain]
    │
    ├─── Phase 2 (Order Service)     ─┐ [LLD §2.4 actor/state machine design]
    │                                  ├── Phase 7 (Tests)
    ├─── Phase 3 (Inventory Service) ─┤ [LLD §2.5 stream pipeline design]
    │                                  │
    └─── Phase 4 (Notification Svc)  ─┘ [LLD §2.6 actor hierarchy]
             │
         Phase 5 (Dockerfile + Docker Compose)
             │
         Phase 6 (Kubernetes)           [HLD §1.7 deployment topology]
```

Phases 2, 3, 4 are independent and can be built in parallel.

---

# 4. Improvements & Suggestions <a name="improvements"></a>

### Critical (fix before writing any service code)

1. **Resolve dual build system** — separate SBT and Maven scopes as described in Phase 0.1
2. **Move source into service directories** — eliminate root `src/` duplication

### Architecture

3. **Choreography-based Saga** — Order creation spans 3 services. Implement compensating transactions via Kafka events:
   - Inventory reservation fails → publish `order.cancel.requested` → Order Service cancels order
   - Notification fails → retry via DLT, do not cancel order (non-critical path)

4. **Event sourcing for Order Service** — Akka Persistence gives a free audit trail of every state change. Natural fit given Akka Actors are already chosen.

5. **Idempotency on `POST /orders`** — Accept `Idempotency-Key` header. Store in Redis with 24h TTL. Return cached response on duplicate request. Prevents double-orders on network retries.

### Observability

6. **OpenTelemetry** — Add OTel Java agent to all 4 services. Route traces to Jaeger or Zipkin. This is essential for debugging cross-service Kafka flows — without it, tracing a request through 4 services is nearly impossible.

7. **Structured JSON logging** — Use Logback JSON encoder (Scala: `logstash-logback-encoder`). Inject `traceId`, `spanId`, `orderId` into MDC for log correlation.

8. **Kafka consumer lag HPA** — Configure KEDA (Kubernetes Event-driven Autoscaling) to scale Inventory Service based on `order.created` consumer group lag. Prevents queue buildup during order spikes.

### Security (OWASP Top 10)

9. **JWT at gateway** — Validate and reject at the gateway filter layer. Downstream services should not receive unauthenticated requests. Use RS256 (asymmetric), not HS256.

10. **Secrets management** — Never put credentials in ConfigMaps or environment variables in plain text. Use `external-secrets-operator` with a vault backend, or at minimum K8s Secrets (base64 is not encryption — treat as plain text).

11. **Input validation** — `@Valid` + Jakarta Bean Validation on all request bodies at the gateway. Prevents injection and oversized payload attacks (OWASP A03, A04).

12. **SQL injection** — Slick and Spring Data JPA use parameterized queries by default. Do not use raw string interpolation in any query.

### Resilience

13. **Dead Letter Topics** — Already included in Phase 4. Apply the same pattern to Inventory Service consumer. Never lose a message silently.

14. **Database connection pooling** — HikariCP (default in Spring Boot) for Java services. Configure `maximumPoolSize` per service based on expected concurrency. Slick also needs explicit connection pool config.

15. **Optimistic locking on inventory** — Use the `version` column to prevent race conditions when multiple pods update the same product concurrently. Retry on `OptimisticLockException`.

---

# 5. Verification Checklist <a name="verify"></a>

- [ ] `docker-compose up` — all 9 containers healthy (postgres, redis, kafka, kafka-init, kafka-ui, api-gateway, order-service, inventory-service, notification-service)
- [ ] All 9 Kafka topics created: `order.created`, `order.cancelled`, `order.return.requested`, `order.returned`, `inventory.updated`, `notif.sent`, `order.cancel.requested`, `order.created.DLT`, `refund.requested`
- [ ] `POST /api/orders` via Swagger UI (`http://localhost:8080/swagger-ui.html`) returns 201
- [ ] **Kafka UI** — `order.created` message visible at `http://localhost:8090` (login: admin/admin)
- [ ] **Kafka UI** — `inventory.updated` appears within 5s of order creation
- [ ] **Kafka UI** — `notif.sent` appears within 10s
- [ ] **Kafka UI** — consumer group `inventory-service` lag = 0 after processing
- [ ] `GET /api/orders/{id}` returns `status: CONFIRMED`
- [ ] `sbt test` — all Scala unit + integration tests pass
- [ ] `mvn test` — all Java unit tests pass
- [ ] `kubectl apply -f k8s/` — all pods in `Running` state
- [ ] HPA active and showing current CPU metrics
- [ ] Circuit breaker fallback triggers when order-service is stopped

---

## Kafka UI — What You Can Monitor at `http://localhost:8090`

Login: `admin` / `admin` (set in docker-compose `AUTH_TYPE: LOGIN_FORM`)

### Topics view (`/topics`)

| Topic | What to check |
|-------|--------------|
| `order.created` | Message count grows on each `POST /api/orders`; see full JSON payload with `orderId`, `customerId`, `items` |
| `order.cancelled` | Appears when order is cancelled; check `wasConfirmed` and `reason` fields |
| `inventory.updated` | Appears within ~2s of `order.created`; check `status: RESERVED` or `REJECTED` |
| `notif.sent` | Appears after notification dispatched; check `channel` and `status: DELIVERED` |
| `order.created.DLT` | **Should always be empty** — any message here means a consumer crashed 3 times; investigate immediately |
| `refund.requested` | Appears only on return approval; check `refundAmount` matches order total |

**How to browse messages** in Kafka UI:
1. Click any topic name → **Messages** tab
2. Set **Offset**: `Earliest` to see all messages from start
3. Set **Value format**: `JSON` — messages auto-pretty-printed
4. Use **Filter** box to search by `orderId` within message content

### Consumer Groups view (`/consumer-groups`)

| Consumer Group | Service | Lag to watch |
|----------------|---------|-------------|
| `inventory-service` | Inventory Service | Should be 0 at rest; spikes during load are OK if it recovers |
| `notification-service` | Notification Service | Should be 0; non-zero means notifications are queuing up |

**Consumer lag = 0** means the service is keeping up with incoming orders.  
**Consumer lag growing** means the service is overwhelmed — time to scale up (KEDA handles this in K8s).

### Broker view (`/brokers`)

- Confirm 1 broker online (local dev) / 3 brokers (prod)
- Check `Under-replicated partitions` = 0

### Live event trace — full order flow

Open Kafka UI, go to `order.created` → Messages → Earliest, then in a separate tab:

```bash
curl -X POST http://localhost:8080/api/orders \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer <jwt>" \
  -H "Idempotency-Key: $(uuidgen)" \
  -d '{"customerId":"3fa85f64-5717-4562-b3fc-2c963f66afa6",
       "items":[{"productId":"prod-001","quantity":2,"unitPrice":29.99}],
       "shippingAddress":{"street":"123 Main","city":"NYC","country":"US","zip":"10001"}}'
```

Then watch in Kafka UI (refresh Messages):
```
order.created      → message appears immediately
inventory.updated  → appears ~2s later  (status: RESERVED)
notif.sent         → appears ~3s later  (channel: EMAIL, status: DELIVERED)
```

If `inventory.updated` shows `status: REJECTED` → check `order.cancel.requested` topic for the saga compensation event.

