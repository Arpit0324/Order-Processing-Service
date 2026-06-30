# Notification Service

Sends multi-channel notifications (email, SMS) in response to Kafka events from the order lifecycle. Built with **Java 21 + Spring Boot 3 + Pekko Typed**. Kafka messages are consumed by Spring `@KafkaListener` and dispatched to Pekko actors for async delivery.

---

## Responsibilities

- Consume `order.created` → send order confirmation email/SMS
- Consume `order.cancelled` → send cancellation email (reason-aware messaging)
- Consume `order.cancel.requested` → send out-of-stock alert
- Consume `order.returned` → send return approval + refund initiated email
- Publish `notif.sent` after each successful dispatch
- Persist all notification records to DB (audit log)
- REST API for notification history lookup

---

## Architecture

```
Kafka Topics
    │
    ▼
NotificationConsumer (@KafkaListener)
    │  @RetryableTopic: 3 retries, exponential backoff
    │  Failed after 3 → order.created.DLT
    ▼
NotificationServiceImpl (@Service, @Transactional)
    │
    ├─► NotificationRepository (persist record to DB)
    │
    ├─► NotificationRouter (Pekko Typed actor)
    │       │
    │       ├─► EmailNotificationActor (pool × 4)
    │       └─► SmsNotificationActor   (pool × 2)
    │
    └─► NotificationEventProducer → Kafka: notif.sent
```

---

## Technology

| Component | Technology |
|-----------|-----------|
| Language | Java 21 |
| Web framework | Spring Boot 3.3.1 |
| Kafka consumer | Spring Kafka with `@RetryableTopic` |
| Actor model | Pekko Typed 1.1.3 (Java API) |
| Database | Spring Data JPA + PostgreSQL |
| DB migrations | Flyway 10.15.0 (auto-run by Spring Boot) |
| JSON | Jackson with JSR-310 (Instant support) |

---

## Running Locally

### With Docker Compose (recommended)

```bash
# From project root
docker-compose up notification-service postgres kafka kafka-init
```

### Standalone

```bash
# From project root — requires Postgres and Kafka running
mvn -pl notification-service spring-boot:run
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `SERVER_PORT` | `8083` | HTTP server port |
| `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/ops_notifications` | PostgreSQL URL |
| `SPRING_DATASOURCE_USERNAME` | `ops` | DB username |
| `SPRING_DATASOURCE_PASSWORD` | `changeme` | DB password |
| `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka bootstrap servers |

---

## API Endpoints

Base URL: `http://localhost:8083` (direct) or `http://localhost:8080/api/notifications` (via gateway)

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/notifications?orderId={id}` | Get all notifications for an order |
| `GET` | `/notifications/{id}` | Get a single notification by ID |
| `GET` | `/actuator/health` | Liveness + readiness probe |

### List Notifications — Example

```bash
curl "http://localhost:8083/notifications?orderId=ord-abc123" \
  -H "X-Trace-Id: trace-xyz"

# Response:
# [
#   {
#     "id": "notif-uuid",
#     "orderId": "ord-abc123",
#     "channel": "EMAIL",
#     "template": "ORDER_CONFIRMED",
#     "status": "DELIVERED",
#     "sentAt": "2026-06-29T10:30:03Z"
#   }
# ]
```

---

## Kafka Topics

| Direction | Topic | When |
|-----------|-------|------|
| **Consumes** | `order.created` | Order placed |
| **Consumes** | `order.cancelled` | Order cancelled |
| **Consumes** | `order.cancel.requested` | Out-of-stock cancellation |
| **Consumes** | `order.returned` | Return approved |
| **Produces** | `notif.sent` | After notification dispatched |

### Dead Letter Topic

If a message fails after 3 retries, it is sent to `order.created.DLT`.  
**Monitor this topic** in Kafka UI — any message here indicates a processing failure that needs investigation.

---

## Notification Templates

| Template | Trigger | Channel |
|----------|---------|---------|
| `ORDER_CONFIRMED` | `order.created` consumed | Email + SMS |
| `ORDER_CANCELLED` | `order.cancelled` consumed | Email |
| `ORDER_CANCELLED_OUT_OF_STOCK` | `order.cancel.requested` consumed | Email |
| `RETURN_APPROVED` | `order.returned` consumed | Email + SMS |
| `REFUND_INITIATED` | `order.returned` consumed | Email |

---

## Database

Database: `ops_notifications`

| Table | Description |
|-------|-------------|
| `notifications` | Audit log of every notification sent (channel, template, status, attempts) |

Migrations: `src/main/resources/db/migration/`  
Flyway runs automatically on application startup (before JPA initialises).

---

## Adding a New Notification Channel

1. Add a new `record` to `NotificationCommand.java` (sealed interface)
2. Add a new actor in `actor/` implementing the channel
3. Add a `Behaviors.receive` case in `NotificationRouter.java`
4. Call the new command from `NotificationServiceImpl`

---

## Build

```bash
# From project root
mvn -pl notification-service package -DskipTests
mvn -pl notification-service test
docker build -t ops/notification-service ./notification-service
```
