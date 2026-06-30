# Order Processing System

A production-grade polyglot microservices system for order lifecycle management, built with Scala + Apache Pekko, Java + Spring Boot, Kafka, PostgreSQL, Redis, and Kubernetes.

---

## Architecture

```
Client / Browser
      │
      ▼
┌──────────────────────────────────────────┐
│   API Gateway  :8080                     │
│   Spring Cloud Gateway + Swagger UI      │
│   JWT Auth · Rate Limit · Circuit Breaker│
└──────┬──────────┬──────────┬─────────────┘
       │          │          │
       ▼          ▼          ▼
 Order Svc   Inventory   Notification
 :8081       Svc :8082   Svc :8083
 Scala +     Scala +     Java +
 Pekko Actors Pekko Streams Spring + Pekko
       │          │          │
       └──────────┼──────────┘
                  ▼
        ┌──────────────────┐
        │   Kafka :9092    │  9 topics
        └──────────────────┘
                  │
        ┌─────────┴────────┐
        ▼                  ▼
   PostgreSQL :5432     Redis :6379
   3 databases          Cache + IdempotencyKeys
```

---

## Services

| Service | Language | Port | Description |
|---------|----------|------|-------------|
| [api-gateway](./api-gateway/README.md) | Java 21 + Spring Boot 3 | 8080 | JWT auth, routing, rate-limit, circuit breaker, Swagger |
| [order-service](./order-service/README.md) | Scala 3 + Pekko Typed | 8081 | Order lifecycle with event sourcing, saga compensation |
| [inventory-service](./inventory-service/README.md) | Scala 3 + Pekko Streams | 8082 | Stock reservation, reactive Kafka pipeline, Redis cache |
| [notification-service](./notification-service/README.md) | Java 21 + Spring Boot 3 | 8083 | Multi-channel notifications (email/SMS), Kafka consumer |
| [shared](./shared/README.md) | Scala 3 | — | Kafka event schemas, JSON codecs, domain value objects |

---

## Local Development

### Prerequisites

| Tool | Minimum version | Check |
|------|----------------|-------|
| Docker Desktop | 4.x | `docker --version` |
| JDK | 21 | `java -version` |
| SBT | 1.10 | `sbt --version` |
| Maven | 3.9 | `mvn --version` |

---

### Step 1 — Compile

The project uses two build tools: **SBT** for the Scala services and **Maven** for the Java services.

```bash
# Scala services: shared + order-service + inventory-service
sbt compile

# Java services: api-gateway + notification-service
mvn compile
```

Compile a single service:

```bash
sbt "orderService/compile"          # order-service only
sbt "inventoryService/compile"      # inventory-service only
mvn -pl api-gateway compile         # api-gateway only
mvn -pl notification-service compile  # notification-service only
```

---

### Step 2 — Test

#### Scala services (ScalaTest + optional Testcontainers)

```bash
# All Scala tests
sbt test

# Per-service
sbt "orderService/test"
sbt "inventoryService/test"
sbt "shared/test"

# Single spec
sbt "orderService/testOnly com.ops.order.domain.OrderSpec"
sbt "orderService/testOnly com.ops.order.domain.OrderStatusSpec"

# Watch mode — re-runs on every file save
sbt "~orderService/test"
```

JUnit XML reports are written to `<service>/target/test-reports/` for CI.

#### Java services (JUnit 5 + Spring Boot Test)

```bash
# All Java tests
mvn test

# Per-service
mvn -pl api-gateway test
mvn -pl notification-service test

# Skip tests (build only)
mvn package -DskipTests
```

> **Note:** Integration tests that use Testcontainers require Docker to be running.

---

### Step 3 — Run

#### Option A — Full stack with Docker Compose (recommended)

```bash
# Build images and start all 9 containers
docker-compose up --build
```

Containers started:

| Container | Port | Description |
|-----------|------|-------------|
| `postgres` | 5432 | 3 databases: `ops_orders`, `ops_inventory`, `ops_notifications` |
| `redis` | 6379 | Cache + rate-limit counters |
| `kafka` | 9092 | KRaft mode (no Zookeeper) |
| `kafka-init` | — | Creates 9 Kafka topics then exits |
| `kafka-ui` | 8090 | Kafka topic browser |
| `api-gateway` | 8080 | Spring Cloud Gateway |
| `order-service` | 8081 | Pekko Typed persistent actors |
| `inventory-service` | 8082 | Pekko Streams reactive pipeline |
| `notification-service` | 8083 | Spring Boot + Pekko |

#### Option B — Infra only, services from source

Useful when iterating on a single service without rebuilding Docker images.

```bash
# 1. Start only the backing infrastructure
docker-compose up -d postgres redis kafka kafka-init

# 2a. Run a Scala service via SBT (pick one)
sbt "orderService/run"
sbt "inventoryService/run"

# 2b. Run a Java service via Maven (pick one)
mvn -pl api-gateway spring-boot:run
mvn -pl notification-service spring-boot:run

# 2c. Or run a fat JAR built earlier
java -jar order-service/target/scala-3.4.2/order-service.jar
java -jar api-gateway/target/api-gateway-*.jar
```

#### Option C — IDE

Run these main classes directly from IntelliJ or VS Code:

| Service | Main class |
|---------|------------|
| order-service | `com.ops.order.OrderServiceApp` |
| inventory-service | `com.ops.inventory.InventoryServiceApp` |
| api-gateway | `com.ops.gateway.ApiGatewayApplication` |
| notification-service | `com.ops.notification.NotificationApplication` |

Ensure the environment variables in the table below are set in each run configuration.

---

### Step 4 — Verify everything is up

```bash
curl http://localhost:8080/actuator/health   # API Gateway
curl http://localhost:8081/health            # Order Service
curl http://localhost:8082/health            # Inventory Service
curl http://localhost:8083/actuator/health   # Notification Service
```

All four should return `200 OK` before sending any API traffic.

---

### Environment Variables

All services start with sensible defaults — no `.env` file is required for local dev with Docker Compose.
When running services standalone (Option B/C), set these as needed:

| Variable | Default | Used by |
|----------|---------|--------|
| `HTTP_PORT` | `8081` / `8082` / `8080` / `8083` | each service |
| `DB_URL` | `jdbc:postgresql://localhost:5432/<db>` | order, inventory, notification |
| `DB_USER` | `ops` | order, inventory, notification |
| `DB_PASSWORD` | `changeme` | order, inventory, notification |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | all services |
| `REDIS_HOST` | `localhost` | inventory-service |
| `JWT_SECRET` | `dev-secret` | api-gateway |

---

### Stop and clean up

```bash
# Stop all containers (keep volumes)
docker-compose down

# Stop and wipe all data volumes (full reset)
docker-compose down -v
```

---

## Important URLs

| URL | Description |
|-----|-------------|
| http://localhost:8080/swagger-ui.html | **Swagger UI** — try all API endpoints here |
| http://localhost:8080/v3/api-docs | OpenAPI 3.0 spec (JSON) |
| http://localhost:8090 | **Kafka UI** — browse topics, consumer groups, messages (login: `admin`/`admin`) |
| http://localhost:8080/actuator/health | Gateway health |
| http://localhost:8080/actuator/circuitbreakers | Circuit breaker state |
| http://localhost:8080/actuator/metrics | Prometheus metrics |

---

## API Overview

All endpoints require `Authorization: Bearer <JWT>` (RS256).

```
POST   /api/orders                       Create a new order
GET    /api/orders?customerId={id}       List orders for a customer
GET    /api/orders/{id}                  Get order by ID
DELETE /api/orders/{id}                  Cancel an order

POST   /api/orders/{id}/return           Request a return (FULFILLED only, 30-day window)
PUT    /api/orders/{id}/return/approve   Approve return (ops)
PUT    /api/orders/{id}/return/reject    Reject return (ops)

GET    /api/inventory/{productId}        Get stock level
PUT    /api/inventory/{productId}/stock  Adjust stock (ops)

GET    /api/notifications?orderId={id}   List notifications for an order
GET    /api/notifications/{id}           Get notification by ID
```

---

## Order Lifecycle

```
PENDING ──── ConfirmOrder ──── CONFIRMED ──── FulfillOrder ──── FULFILLED
   │                │                                               │
   │ CancelOrder    │ CancelOrder (saga)                     RequestReturn
   ▼                ▼                                               ▼
CANCELLED       CANCELLED                                  RETURN_REQUESTED
                                                             │          │
                                                       Approve      Reject
                                                             ▼          ▼
                                                         RETURNED   FULFILLED
```

Stale `PENDING` orders auto-cancel after **15 minutes** via Pekko timer.

---

## Event Flow

```
POST /api/orders
  → Kafka: order.created
    → Inventory Service: reserve stock
      → Kafka: inventory.updated (RESERVED)
      → Order Service: confirm order
      OR
      → Kafka: order.cancel.requested  (insufficient stock — saga)
        → Order Service: cancel order
    → Notification Service: send confirmation
      → Kafka: notif.sent
```

---

## Kafka Topics

| Topic | Partitions | Consumers |
|-------|-----------|-----------|
| `order.created` | 3 | inventory-service, notification-service |
| `order.cancelled` | 3 | inventory-service, notification-service |
| `order.cancel.requested` | 3 | order-service, notification-service |
| `order.return.requested` | 1 | notification-service |
| `order.returned` | 1 | inventory-service, notification-service |
| `inventory.updated` | 3 | order-service |
| `notif.sent` | 1 | — (audit only) |
| `order.created.DLT` | 1 | — (dead letter — alert if non-empty) |
| `refund.requested` | 1 | — (payment service integration) |

---

## Project Structure

```
order-processing-system/
├── build.sbt                 SBT multi-project (shared, order-service, inventory-service)
├── pom.xml                   Maven parent (api-gateway, notification-service)
├── docker-compose.yml        Full local dev stack (9 containers)
├── .env.example              Environment variable template
├── plan.md                   Full HLD + LLD + architecture documentation
│
├── shared/                   Kafka event schemas (Scala — imported by all services)
├── order-service/            Scala 3 + Pekko Typed + Pekko Persistence
├── inventory-service/        Scala 3 + Pekko Streams + Redis
├── notification-service/     Java 21 + Spring Boot 3 + Pekko
├── api-gateway/              Java 21 + Spring Cloud Gateway
│
├── infra/postgres/           PostgreSQL multi-database init script
└── k8s/                      Kubernetes manifests (namespace, deployments, services, HPA)
```

---

## Database Schema

Each service owns its own database (database-per-service pattern):

| Database | Service | Schema managed by |
|----------|---------|-------------------|
| `ops_orders` | order-service | Flyway (`V1–V2` migrations) |
| `ops_inventory` | inventory-service | Flyway (`V1` migration) |
| `ops_notifications` | notification-service | Flyway (`V1` migration) |

---

## Kubernetes Deployment

```bash
# Apply all manifests
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmaps/
kubectl apply -f k8s/secrets/        # Update with real secrets first!
kubectl apply -f k8s/deployments/
kubectl apply -f k8s/services/
kubectl apply -f k8s/ingress/
kubectl apply -f k8s/hpa/

# Check status
kubectl get pods -n ops-system
kubectl get hpa -n ops-system
```

**Important**: Replace placeholder base64 values in [k8s/secrets/secrets.yaml](k8s/secrets/secrets.yaml) before deploying. Use `external-secrets-operator` or Sealed Secrets in production.

---

## Build Reference

```bash
# ── Scala (SBT) ──────────────────────────────────────────────────────────────
sbt compile                           # Compile all Scala modules
sbt test                              # Test all Scala modules
sbt clean assembly                    # Build all fat JARs
sbt "orderService/assembly"           # Fat JAR for order-service only
sbt "inventoryService/assembly"       # Fat JAR for inventory-service only

# ── Java (Maven) ─────────────────────────────────────────────────────────────
mvn compile                           # Compile all Java modules
mvn test                              # Test all Java modules
mvn clean package -DskipTests         # Build all JARs (skip tests)
mvn -pl api-gateway package           # Build only api-gateway
mvn -pl notification-service package  # Build only notification-service

# ── Docker images ─────────────────────────────────────────────────────────────
docker build -t ops/order-service        ./order-service
docker build -t ops/inventory-service    ./inventory-service
docker build -t ops/notification-service ./notification-service
docker build -t ops/api-gateway          ./api-gateway
```

---

## Tech Stack

| Technology | Version | Used For |
|-----------|---------|---------|
| Scala | 3.4.2 | Order Service, Inventory Service, Shared |
| Java | 21 | API Gateway, Notification Service |
| Apache Pekko Typed | 1.1.3 | Actor model, event sourcing, streaming |
| Spring Boot | 3.3.1 | Gateway, Notification REST + Kafka |
| Spring Cloud Gateway | 2023.0.2 | Reactive proxy + circuit breaker |
| Apache Kafka | 7.6.1 (KRaft) | Async event bus |
| PostgreSQL | 16 | Primary data store |
| Redis | 7 | Cache + rate-limit + idempotency |
| Flyway | 10.15.0 | DB schema migrations |
| Docker | — | Containerisation |
| Kubernetes | — | Production orchestration |
