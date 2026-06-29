# Shared Module

Contains the **Kafka event schemas, JSON codecs, and domain value objects** shared by all services. This module is the single source of truth for all inter-service message contracts.

---

## Contents

### Domain Value Objects (`domain/`)

| Class | Description |
|-------|-------------|
| `Money` | Currency-aware amount with addition and validation |
| `ItemLine` | Order line item: productId, quantity, unitPrice |
| `ShippingAddress` | Street, city, country, zip |

### Kafka Event Types (`events/`)

All events implement the `KafkaEvent` sealed trait, which enforces:
- `eventId` — unique UUID per event
- `eventVersion` — for schema evolution
- `occurredAt` — ISO-8601 timestamp
- `traceId` — OpenTelemetry trace ID for cross-service correlation

| Event Class | Topic | Published By |
|-------------|-------|-------------|
| `OrderCreatedEvent` | `order.created` | order-service |
| `OrderCancelledEvent` | `order.cancelled` | order-service |
| `OrderCancelRequestedEvent` | `order.cancel.requested` | inventory-service |
| `OrderReturnRequestedEvent` | `order.return.requested` | order-service |
| `OrderReturnedEvent` | `order.returned` | order-service |
| `InventoryUpdatedEvent` | `inventory.updated` | inventory-service |
| `NotificationSentEvent` | `notif.sent` | notification-service |
| `RefundRequestedEvent` | `refund.requested` | order-service |

### JSON Codecs (`serialization/`)

Circe-based codecs for all event types. Import `com.ops.shared.serialization.JsonCodecs.given` in any Scala service.

Java services (api-gateway, notification-service) use Jackson for deserialization — the JSON field names must match exactly.

---

## Usage in Scala Services

```scala
import com.ops.shared.events.*
import com.ops.shared.serialization.JsonCodecs.given
import io.circe.syntax.*
import io.circe.parser.*

// Serialize
val event = OrderCreatedEvent(...)
val json  = event.asJson.noSpaces

// Deserialize
val result = decode[OrderCreatedEvent](jsonString)
```

---

## Build

```bash
# From project root
sbt "shared/compile"
sbt "shared/test"

# Publish to local Maven repo (for Java services to consume as JAR)
sbt "shared/publishLocal"
```

---

## Schema Evolution

**Never change field names or types in existing event versions.** Instead:

1. Bump `eventVersion` in the new event case class
2. Add a new event class (e.g., `OrderCreatedEventV2`)
3. Consumers check `eventVersion` and handle both versions during rollout
4. Remove old version after all consumers are updated

This follows the **backward-compatible schema evolution** rule documented in `plan.md`.
