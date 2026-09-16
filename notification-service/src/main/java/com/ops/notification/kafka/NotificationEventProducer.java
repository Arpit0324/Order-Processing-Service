package com.ops.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.notification.domain.NotificationRecord;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

// ── Publishes notif.sent event after each successful notification delivery ────
// Called from the service layer after the record transitions to DELIVERED.
// Transient publish failures are retried with backoff (3 attempts); terminal
// failures are logged at ERROR for alerting (notif.sent currently has no
// downstream consumers, so a lost event affects audit only).
@Component
public class NotificationEventProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventProducer.class);
    private static final String TOPIC = "notif.sent";
    private static final int    MAX_ATTEMPTS = 3;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper                  mapper;
    private final ScheduledExecutorService      retryExecutor;

    public NotificationEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                                      ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper        = mapper;
        this.retryExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "notif-sent-retry");
            t.setDaemon(true);
            return t;
        });
    }

    public void publishNotificationSent(NotificationRecord record, String traceId) {
        final String json;
        try {
            json = mapper.writeValueAsString(buildEvent(record, traceId));
        } catch (Exception ex) {
            log.error("Serialization failed for notif.sent notifId={}", record.getId(), ex);
            return;
        }
        sendWithRetry(record, json, 1);
    }

    private void sendWithRetry(NotificationRecord record, String json, int attempt) {
        kafkaTemplate.send(TOPIC, record.getOrderId(), json)
            .whenComplete((result, ex) -> {
                if (ex == null) {
                    log.debug("Published notif.sent notifId={} offset={}",
                            record.getId(), result.getRecordMetadata().offset());
                    return;
                }
                if (attempt < MAX_ATTEMPTS) {
                    long backoffMs = 500L * attempt;
                    log.warn("notif.sent publish failed notifId={} attempt={}/{} — retrying in {}ms: {}",
                            record.getId(), attempt, MAX_ATTEMPTS, backoffMs, ex.getMessage());
                    retryExecutor.schedule(() -> sendWithRetry(record, json, attempt + 1),
                            backoffMs, TimeUnit.MILLISECONDS);
                } else {
                    log.error("notif.sent publish permanently failed notifId={} orderId={} after {} attempts",
                            record.getId(), record.getOrderId(), MAX_ATTEMPTS, ex);
                }
            });
    }

    private Map<String, Object> buildEvent(NotificationRecord record, String traceId) {
        return Map.of(
            "eventId",      UUID.randomUUID().toString(),
            "eventType",    "NOTIFICATION_SENT",
            "eventVersion", 1,
            "occurredAt",   Instant.now().toString(),
            "traceId",      traceId,
            "payload", Map.of(
                "notifId",   record.getId(),
                "orderId",   record.getOrderId(),
                "channel",   record.getChannel(),
                "recipient", record.getRecipient(),
                "template",  record.getTemplate(),
                "status",    record.getStatus()
            )
        );
    }

    @PreDestroy
    void shutdown() {
        retryExecutor.shutdown();
    }
}
