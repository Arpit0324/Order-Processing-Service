## Plan: Notification Service Design Hardening

TL;DR: Keep the Kafka-driven, DB-audited notification architecture, but fix the reliability model first: single Pekko `ActorSystem`, idempotent consumption, explicit actor acknowledgements, durable dead-letter handling, real recipient resolution, and only then add channel providers, scaling, observability, and contract tests.

### Current architecture

- Consumes `order.created`, `order.cancelled`, `order.cancel.requested`, and `order.returned` through `NotificationConsumer`.
- Persists a `NotificationRecord` as `PENDING` through `NotificationServiceImpl`.
- Dispatches fire-and-forget commands to a Pekko router with email/SMS actor pools.
- Publishes `notif.sent` through `NotificationEventProducer`.
- Exposes read-only audit endpoints through `NotificationController`.
- Deploys as two fixed Kubernetes replicas with PostgreSQL, Flyway, Actuator, and API Gateway routing.

### Key design pitfalls

1. **Broken actor lifecycle**: `AkkaConfig` creates more than one actor system / root actor path, so router lifecycle and shutdown are not cleanly owned by Spring.
2. **At-most-once behavior disguised as retryable processing**: Kafka offsets can be acknowledged before asynchronous actor work completes.
3. **No idempotency**: repeated Kafka delivery can duplicate records and duplicate customer notifications.
4. **No delivery feedback loop**: records remain `PENDING` because actor success/failure never updates the database.
5. **Unreliable event publication**: `notif.sent` publish failures are logged but not propagated or retried.
6. **DLT is a dead end**: exhausted messages are only logged, not persisted, alerted, or replayed.
7. **Core delivery is stubbed**: recipient email is a placeholder and email/SMS actors only log.
8. **Transactional boundary is wrong**: persistence, actor dispatch, and Kafka publication are not coordinated as a reliable unit.
9. **Retry data model exists but no retry process uses it**: `findRetryable()` is unused.
10. **Limited resilience and operability**: no circuit breakers, no actor health/metrics, incomplete health checks, fixed pool sizes, missing provider secrets, and no HPA.
11. **Contract risk**: event schemas and the intended `CustomerClient` contract are not versioned or documented.

### Recommended remediation

#### Phase 1 — Correctness and reliability first

1. Fix Pekko ownership in `notification-service/src/main/java/com/ops/notification/config/AkkaConfig.java`: create one Spring-managed typed `ActorSystem`, spawn `NotificationRouter` as a child, and expose router and system through separate beans.
2. Change consumption flow in `notification-service/src/main/java/com/ops/notification/consumer/NotificationConsumer.java` and `notification-service/src/main/java/com/ops/notification/service/NotificationServiceImpl.java` so Kafka acknowledgment occurs only after durable processing reaches a terminal or safely retryable state.
3. Add an idempotency key based on event identity such as `(eventId, orderId, template, channel)`; add a matching unique constraint in `notification-service/src/main/resources/db/migration/V1__create_notifications_table.sql` and handle constraint violations as duplicate delivery.
4. Replace fire-and-forget dispatch with explicit actor acknowledgements: actor replies success/failure, service updates `DELIVERED` or `FAILED`, and failures become retryable.
5. Make `notification-service/src/main/java/com/ops/notification/kafka/NotificationEventProducer.java` failure-aware: retry transient publish errors and mark/alert terminal failures.
6. Persist DLT payload, topic, partition, offset, exception, trace ID, and retry metadata to a new dead-letter table; expose replay tooling or an admin endpoint.

#### Phase 2 — Make the service actually deliver notifications

7. Replace placeholder recipient resolution in `notification-service/src/main/java/com/ops/notification/consumer/NotificationConsumer.java`. Prefer carrying a recipient snapshot in order events; otherwise introduce a versioned `CustomerClient` contract with timeout, retry, circuit breaker, and cache.
8. Implement email delivery in `notification-service/src/main/java/com/ops/notification/actor/EmailNotificationActor.java` using a configured provider such as SMTP, SES, or SendGrid.
9. Implement SMS delivery in `notification-service/src/main/java/com/ops/notification/actor/SmsNotificationActor.java` using a configured provider such as Twilio.
10. Add provider credentials to Kubernetes secrets and local configuration; keep secrets out of `application.yml` and logs.
11. Add a scheduled retry worker that uses `NotificationRepository.findRetryable()`, bounded attempts, exponential backoff, and terminal DLT transition.

#### Phase 3 — Resilience, scale, and observability

12. Add circuit breakers and bulkheads around customer lookup, email, SMS, database, and Kafka producer boundaries.
13. Externalize actor pool sizes, retry intervals, timeouts, and provider settings in `notification-service/src/main/resources/application.yml` and Kubernetes config.
14. Add actor health indicators, mailbox depth, restart count, processing latency, delivery success/failure counters, and DLT gauges to Micrometer/Prometheus.
15. Extend health checks so Kubernetes reflects Kafka, database, Flyway, and actor-system health.
16. Add an HPA or KEDA scaler for notification-service based on CPU and Kafka consumer lag.
17. Propagate trace context through consumer, service, actor, provider, and producer paths.

#### Phase 4 — Contracts and verification

18. Version event schemas and document ownership for `OrderCreatedEvent`, cancellation/return events, and `NotificationSentEvent`.
19. Add contract tests between producers and notification-service consumers.
20. Add Pekko TestKit tests, repository uniqueness tests, retry/DLT tests, provider integration tests, and Testcontainers end-to-end tests from Kafka input through DB state and `notif.sent` output.
21. Add load/failure tests covering duplicate delivery, broker restart, DB outage, provider outage, actor restart, and DLT replay.

### Relevant files

- `notification-service/src/main/java/com/ops/notification/NotificationApp.java` — application entry point; validate startup wiring after actor configuration changes.
- `notification-service/src/main/java/com/ops/notification/config/AkkaConfig.java` — fix single actor-system ownership and router bean creation.
- `notification-service/src/main/java/com/ops/notification/consumer/NotificationConsumer.java` — idempotency, acknowledgment timing, DLT persistence, recipient resolution.
- `notification-service/src/main/java/com/ops/notification/service/NotificationServiceImpl.java` — transactional boundary, actor acknowledgement handling, status transitions, retry scheduling.
- `notification-service/src/main/java/com/ops/notification/actor/NotificationRouter.java` — routee lifecycle, acknowledgement protocol, ordering, metrics.
- `notification-service/src/main/java/com/ops/notification/actor/EmailNotificationActor.java` — real provider integration, circuit breaker, delivery result.
- `notification-service/src/main/java/com/ops/notification/actor/SmsNotificationActor.java` — real provider integration, circuit breaker, delivery result.
- `notification-service/src/main/java/com/ops/notification/kafka/NotificationEventProducer.java` — retries and terminal publish-failure handling.
- `notification-service/src/main/java/com/ops/notification/repository/NotificationRepository.java` — idempotency lookup, retry queries, DLT persistence.
- `notification-service/src/main/java/com/ops/notification/domain/NotificationRecord.java` and `notification-service/src/main/resources/db/migration/V1__create_notifications_table.sql` — unique constraint, delivery metadata, DLT fields or separate DLT entity.
- `notification-service/src/main/resources/application.yml` — acknowledgment mode, pool sizes, provider config, actuator details, externalized retry settings.
- `notification-service/Dockerfile` — health check and observability agent if adopted.
- `k8s/deployments/notification-service-deployment.yaml` — probes, resources, env, HPA reference.
- `k8s/secrets/secrets.yaml` and `k8s/configmaps/services-config.yaml` — provider credentials and non-secret service settings.
- `k8s/hpa/hpa.yaml` — add notification-service autoscaling.
- `shared/src/main/scala/com/ops/shared/events/KafkaEvents.scala` — event versioning and recipient snapshot decision.
- `docker-compose.yml` — local provider stubs/secrets, DLT tooling, health consistency.

### Verification

1. Compile and run notification-service unit tests after lifecycle refactor.
2. Run Flyway migration against a disposable PostgreSQL database and verify uniqueness and DLT schema.
3. Use Testcontainers to publish duplicate events and prove one notification record and one provider call.
4. Force actor/provider failure and prove Kafka offset is not acknowledged prematurely, record becomes retryable, and DLT persistence occurs after policy exhaustion.
5. Force `notif.sent` publish failure and prove retry/terminal-failure handling.
6. Run Pekko TestKit tests for success, failure, restart, and acknowledgement behavior.
7. Exercise API Gateway fallback and notification read endpoints.
8. Deploy to local compose and Kubernetes; verify probes fail when Kafka/DB/actor system is unhealthy.
9. Validate Prometheus/Grafana metrics for delivery rate, retry count, DLT count, mailbox depth, restart count, and provider latency.
10. Run a load test that increases Kafka lag and verifies HPA/KEDA scaling and bounded connection-pool usage.

### Decisions and scope boundaries

- Preserve Kafka as the source of notification work and PostgreSQL as the audit/delivery state store.
- Retain Pekko only if it provides value for supervised channel workers; otherwise a simpler Spring `@Async`/queue-based worker is acceptable after the reliability fixes.
- Do not implement provider-specific templates, localization, unsubscribe/compliance, or push notifications in the first pass.
- Do not change order/inventory business behavior except for event contract documentation and optional recipient snapshots.
- Security work is limited to credential storage, trace/log hygiene, and avoiding PII leakage; a full compliance review is out of scope.

### Further considerations

1. **Recipient resolution**: Recommended option is to include a recipient snapshot in order events because it avoids synchronous coupling. Alternative: define a versioned customer lookup API if PII ownership forbids event-carried contact data.
2. **Actor model**: Recommended option is to fix and keep Pekko if supervised backpressure is needed. Alternative: replace it with Spring retryable workers if actor complexity is not justified.
3. **DLT replay**: Recommended option is a small admin/replay endpoint guarded by authorization. Alternative: operational replay from a DLT table via a scheduled/manual job.
