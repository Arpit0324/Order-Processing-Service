# Order Service

Manages the full order lifecycle using **Scala 3 + Akka Typed + Akka Persistence** (event sourcing). Each order is a persistent actor — state is rebuilt from replayed events on restart, giving a complete audit trail for free.

---

## Responsibilities

- Create, confirm, cancel, and fulfill orders
- Event-sourced state machine (7 states)
- Auto-cancel stale PENDING orders after 15 minutes (Akka timer)
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
| Actor model | Akka Typed 2.9.3 |
| Event sourcing | Akka Persistence JDBC |
| HTTP server | Akka HTTP 10.6.3 |
| Kafka producer | Alpakka Kafka 5.0.0 |
| Database ORM | Slick 3.5.1 + HikariCP |
| DB migrations | Flyway 10.15.0 |
| JSON | Circe 0.14.9 |

---

## Running Locally

### With Docker Compose (recommended)

```bash
# From project root
docker-compose up order-service postgres kafka kafka-init
```

### Standalone (requires Postgres + Kafka running)

```bash
# From project root
sbt "orderService/run"
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `HTTP_PORT` | `8081` | HTTP server port |
| `DB_URL` | `jdbc:postgresql://localhost:5432/ops_orders` | PostgreSQL URL |
| `DB_USER` | `ops` | DB username |
| `DB_PASSWORD` | `changeme` | DB password |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka bootstrap servers |

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
| `order_events` | Akka Persistence event journal |
| `order_snapshots` | Akka Persistence snapshot store |

Migrations: `src/main/resources/db/migration/`

---

## Build

```bash
# From project root
sbt "orderService/assembly"           # Build fat JAR
sbt "orderService/test"               # Run unit tests
docker build -t ops/order-service ./order-service
```
