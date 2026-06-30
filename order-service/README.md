# Order Service

Manages the full order lifecycle using **Scala 3 + Pekko Typed + Pekko Persistence** (event sourcing). Each order is a persistent actor — state is rebuilt from replayed events on restart, giving a complete audit trail for free.

---

## Responsibilities

- Create, confirm, cancel, and fulfill orders
- Event-sourced state machine (7 states)
- Auto-cancel stale PENDING orders after 15 minutes (Pekko timer)
- Publish `order.created` / `order.cancelled` / `order.returned` Kafka events
- Consume `inventory.updated` to confirm orders
- Consume `order.cancel.requested` for saga compensation (insufficient stock)
- Handle return requests and approval/rejection

---

## Order State Machine

```
             CreateOrder
                 │
                 ▼
           ┌─────────┐   ConfirmOrder   ┌───────────┐
──────────►│ PENDING │────────────────►│ CONFIRMED │
           └────┬────┘                  └─────┬─────┘
                │ CancelOrder (or SLA timeout) │ FulfillOrder
                ▼                             ▼
           ┌───────────┐           ┌──────────────┐
           │ CANCELLED │           │  FULFILLED   │
           └───────────┘           └──────┬───────┘
                                          │ RequestReturn
                                          ▼
                                   ┌────────────────────┐
                                   │ RETURN_REQUESTED   │
                                   └──────┬─────────────┘
                                    Approve│  │Reject
                                          ▼  ▼
                                     RETURNED / FULFILLED
```

Terminal states: `CANCELLED`, `RETURNED`

---

## Technology

| Component | Technology |
|-----------|-----------|
| Language | Scala 3.4.2 |
| Actor model | Pekko Typed 1.1.3 |
| Event sourcing | Pekko Persistence JDBC 1.1.0 |
| HTTP server | Pekko HTTP 1.1.0 |
| Kafka producer | Pekko Connectors Kafka 1.1.0 |
| Database ORM | Slick 3.5.1 + HikariCP |
| DB migrations | Flyway 10.15.0 |
| JSON | Circe 0.14.9 |

---

## Running Locally

### Prerequisites

| Tool | Minimum version | Check |
|------|----------------|-------|
| JDK | 21 | `java -version` |
| SBT | 1.10 | `sbt --version` |
| Docker Desktop | 4.x | `docker --version` |

---

### 1. Compile

```bash
# From project root — compile order-service and its shared dependency
sbt "orderService/compile"

# Or compile everything (shared + order-service + inventory-service)
sbt compile
```

A successful compile prints no errors and exits with code 0.

---

### 2. Test

Unit tests (pure domain logic, no external dependencies):

```bash
# From project root
sbt "orderService/test"
```

Test results are printed to the console and JUnit XML is written to
`order-service/target/test-reports/` (consumed by CI).

To run a single spec:

```bash
sbt "orderService/testOnly com.ops.order.domain.OrderSpec"
sbt "orderService/testOnly com.ops.order.domain.OrderStatusSpec"
```

To run tests continuously on every file save:

```bash
sbt "~orderService/test"
```

---

### 3. Run

#### Option A — Docker Compose (recommended, zero config)

Starts PostgreSQL, Kafka, and the service together:

```bash
# From project root
docker-compose up order-service postgres kafka kafka-init
```

The service is ready when you see:
```
order-service  | Order Service started on port 8081
```

#### Option B — Standalone (Postgres + Kafka must already be running)

Start the backing services first:

```bash
# From project root — only infra, no application containers
docker-compose up -d postgres kafka kafka-init
```

Then run the service from SBT:

```bash
sbt "orderService/run"
```

Or build and run the fat JAR:

```bash
sbt "orderService/assembly"
java -jar order-service/target/scala-3.4.2/order-service.jar
```

#### Option C — IntelliJ / VS Code

Run the main class `com.ops.order.OrderServiceApp` directly from your IDE.
Ensure the environment variables below are set in the run configuration.

---

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | `8081` | HTTP server port |
| `DB_URL` | `jdbc:postgresql://localhost:5432/ops_orders` | PostgreSQL JDBC URL |
| `DB_USER` | `ops` | DB username |
| `DB_PASSWORD` | `changeme` | DB password |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |

All variables have working defaults — no `.env` file is needed for local dev.

---

### Verify the service is up

```bash
curl http://localhost:8081/health
# Expected: {"status":"ok"}
```

---

## API Endpoints

Base URL: `http://localhost:8081` (direct) or `http://localhost:8080/api/orders` (via gateway)

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/orders` | Create a new order |
| `GET` | `/orders/{id}` | Get order by ID |
| `GET` | `/orders?customerId={id}&page=1&pageSize=20` | List customer orders |
| `DELETE` | `/orders/{id}` | Cancel an order |
| `POST` | `/orders/{id}/return` | Request a return (FULFILLED only) |
| `PUT` | `/orders/{id}/return/approve` | Approve return (ops) |
| `PUT` | `/orders/{id}/return/reject` | Reject return (ops) |
| `GET` | `/health` | Liveness probe |

### Create Order — Example

```bash
curl -X POST http://localhost:8081/orders \
  -H "Content-Type: application/json" \
  -H "X-Trace-Id: $(uuidgen)" \
  -d '{
    "customerId": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
    "items": [{"productId": "prod-001", "quantity": 2, "unitPrice": 29.99}],
    "shippingAddress": {"street": "123 Main St", "city": "NYC", "country": "US", "zip": "10001"}
  }'
```

---

## Kafka Topics

| Direction | Topic | Event |
|-----------|-------|-------|
| **Produces** | `order.created` | On new order |
| **Produces** | `order.cancelled` | On cancel/timeout |
| **Produces** | `order.returned` | On return approved |
| **Produces** | `refund.requested` | On return approved |
| **Consumes** | `inventory.updated` | To confirm/cancel order |
| **Consumes** | `order.cancel.requested` | Saga compensation |

---

## Database

Database: `ops_orders`

| Table | Description |
|-------|-------------|
| `orders` | Order records with full lifecycle columns |
| `order_events` | Pekko Persistence event journal |
| `order_snapshots` | Pekko Persistence snapshot store |

Migrations: `src/main/resources/db/migration/`

---

## Build

```bash
# Compile only
sbt "orderService/compile"

# Run unit tests
sbt "orderService/test"

# Build fat JAR (output: order-service/target/scala-3.4.2/order-service.jar)
sbt "orderService/assembly"

# Build Docker image
docker build -t ops/order-service ./order-service
```
