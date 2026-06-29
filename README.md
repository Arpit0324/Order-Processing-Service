# Order Processing System

A production-grade polyglot microservices system for order lifecycle management, built with Scala + Akka, Java + Spring Boot, Kafka, PostgreSQL, Redis, and Kubernetes.

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
 Akka Actors Akka Streams Spring + Akka
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
| [order-service](./order-service/README.md) | Scala 3 + Akka Typed | 8081 | Order lifecycle with event sourcing, saga compensation |
| [inventory-service](./inventory-service/README.md) | Scala 3 + Akka Streams | 8082 | Stock reservation, reactive Kafka pipeline, Redis cache |
| [notification-service](./notification-service/README.md) | Java 21 + Spring Boot 3 | 8083 | Multi-channel notifications (email/SMS), Kafka consumer |
| [shared](./shared/README.md) | Scala 3 | — | Kafka event schemas, JSON codecs, domain value objects |

---

## Quick Start — Local Development

### Prerequisites

- Docker Desktop 4.x+
- JDK 21 (`java -version`)
- SBT 1.10+ (`sbt --version`)
- Maven 3.9+ (`mvn --version`)

### 1. Setup environment

```bash
cp .env.example .env
# Edit .env if needed (defaults work out of the box)
```

### 2. Build all services

```bash
# Build Scala services (order-service + inventory-service + shared)
sbt clean assembly

# Build Java services (api-gateway + notification-service)
mvn clean package -DskipTests
```

### 3. Start everything

```bash
docker-compose up --build
```

This starts 9 containers:
- `postgres` — 3 databases (`ops_orders`, `ops_inventory`, `ops_notifications`)
- `redis` — cache + rate-limit counters
- `kafka` — KRaft mode (no Zookeeper)
- `kafka-init` — creates all 9 Kafka topics then exits
- `kafka-ui` — browse topics and consumer groups
- `api-gateway` — Spring Cloud Gateway
- `order-service` — Akka Typed persistent actors
- `inventory-service` — Akka Streams reactive pipeline
- `notification-service` — Spring Boot + Akka

### 4. Verify everything is running

```bash
curl http://localhost:8080/actuator/health   # Gateway
curl http://localhost:8081/health            # Order Service
curl http://localhost:8082/health            # Inventory Service
curl http://localhost:8083/actuator/health   # Notification Service
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

Stale `PENDING` orders auto-cancel after **15 minutes** via Akka timer.

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
├── order-service/            Scala 3 + Akka Typed + Akka Persistence
├── inventory-service/        Scala 3 + Akka Streams + Redis
├── notification-service/     Java 21 + Spring Boot 3 + Akka
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

## Build Commands

```bash
# Scala services
sbt clean assembly                    # Build all Scala modules
sbt "orderService/assembly"           # Build only order-service
sbt test                              # Run all Scala tests

# Java services
mvn clean package -DskipTests         # Build all Java modules
mvn -pl api-gateway package           # Build only api-gateway
mvn test                              # Run all Java tests

# Docker (individual service)
docker build -t ops/order-service ./order-service
docker build -t ops/inventory-service ./inventory-service
docker build -t ops/notification-service ./notification-service
docker build -t ops/api-gateway ./api-gateway

# Full stack
docker-compose up --build             # Start all
docker-compose down -v                # Stop + wipe volumes
```

---

## Tech Stack

| Technology | Version | Used For |
|-----------|---------|---------|
| Scala | 3.4.2 | Order Service, Inventory Service, Shared |
| Java | 21 | API Gateway, Notification Service |
| Akka Typed | 2.9.3 | Actor model, event sourcing, streaming |
| Spring Boot | 3.3.1 | Gateway, Notification REST + Kafka |
| Spring Cloud Gateway | 2023.0.2 | Reactive proxy + circuit breaker |
| Apache Kafka | 7.6.1 (KRaft) | Async event bus |
| PostgreSQL | 16 | Primary data store |
| Redis | 7 | Cache + rate-limit + idempotency |
| Flyway | 10.15.0 | DB schema migrations |
| Docker | — | Containerisation |
| Kubernetes | — | Production orchestration |
