package com.ops.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.notification.domain.NotificationRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

// ── Publishes notif.sent event after each notification dispatch ───────────────
// Called from service layer after persisting the record.
@Component
public class NotificationEventProducer {

    private static final Logger log = LoggerFactory.getLogger(NotificationEventProducer.class);
    private static final String TOPIC = "notif.sent";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper                  mapper;

    public NotificationEventProducer(KafkaTemplate<String, String> kafkaTemplate,
                                      ObjectMapper mapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.mapper        = mapper;
    }

    public void publishNotificationSent(NotificationRecord record, String traceId) {
        try {
            Map<String, Object> event = Map.of(
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
            String json = mapper.writeValueAsString(event);
            kafkaTemplate.send(TOPIC, record.getOrderId(), json)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("Failed to publish notif.sent for notifId={}", record.getId(), ex);
                    } else {
                        log.debug("Published notif.sent notifId={} offset={}",
                                record.getId(), result.getRecordMetadata().offset());
                    }
                });
        } catch (Exception ex) {
            log.error("Serialization failed for notif.sent notifId={}", record.getId(), ex);
        }
    }
}
