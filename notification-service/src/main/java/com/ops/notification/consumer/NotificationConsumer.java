package com.ops.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.notification.domain.DeadLetterNotification;
import com.ops.notification.repository.DeadLetterRepository;
import com.ops.notification.service.NotificationService;
import com.ops.notification.service.RecipientResolver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

// ── ENDPOINT (Kafka entry point) — delegates immediately to service layer ─────
// @RetryableTopic: 3 retries with exponential backoff before sending to DLT.
// Ack mode is MANUAL: the offset is committed only after the service reports a
// durable outcome (delivered, or duplicate-skipped). Exceptions skip the ack so
// the retry/DLT machinery can redeliver. Exhausted messages are persisted to
// dead_letter_notifications by the DLT handler.
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationService   service;
    private final ObjectMapper          mapper;
    private final DeadLetterRepository  deadLetterRepository;

    public NotificationConsumer(NotificationService service, ObjectMapper mapper,
                                DeadLetterRepository deadLetterRepository) {
        this.service              = service;
        this.mapper               = mapper;
        this.deadLetterRepository = deadLetterRepository;
    }

    // ── Consumes order.created → send order confirmation ─────────────────────
    @RetryableTopic(
        attempts       = "3",
        backoff        = @Backoff(delay = 1000, multiplier = 2.0, maxDelay = 10000),
        autoCreateTopics = "false"
    )
    @KafkaListener(topics = "order.created", groupId = "notification-service-orders")
    public void onOrderCreated(@Payload String payload,
                               @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
                               @Header(value = "X-Trace-Id", required = false) String traceId,
                               Acknowledgment ack) {
        log.debug("Received order.created message traceId={}", traceId);
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p = getPayload(event);

            String orderId     = str(p, "orderId");
            String customerId  = str(p, "customerId");
            String eventId     = str(event, "eventId");
            String traceIdVal  = traceId != null ? traceId : str(event, "traceId");

            // Recipient: prefer the snapshot carried on the event; fall back to a
            // placeholder until order-service populates customerEmail/customerPhone.
            String email = RecipientResolver.resolveEmail(str(p, "customerEmail"), orderId);
            String phone = RecipientResolver.resolvePhone(str(p, "customerPhone"));

            @SuppressWarnings("unchecked")
            var items = (List<?>) p.get("items");
            var total = new java.math.BigDecimal(str(p, "totalAmount"));

            service.sendOrderConfirmation(orderId, customerId, email, phone, total, traceIdVal, eventId);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed processing order.created: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex); // no ack → @RetryableTopic redelivers
        }
    }

    // ── Consumes order.cancelled → send cancellation notice ──────────────────
    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0),
                    autoCreateTopics = "false")
    @KafkaListener(topics = "order.cancelled", groupId = "notification-service-cancelled")
    public void onOrderCancelled(@Payload String payload,
                                 @Header(value = "X-Trace-Id", required = false) String traceId,
                                 Acknowledgment ack) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId    = str(p, "orderId");
            String customerId = str(p, "customerId");
            String reason     = str(p, "reason");
            String email      = RecipientResolver.resolveEmail(str(p, "customerEmail"), orderId);
            String eventId    = str(event, "eventId");
            String traceIdVal = traceId != null ? traceId : str(event, "traceId");

            service.sendOrderCancellation(orderId, customerId, email, reason, traceIdVal, eventId);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed processing order.cancelled: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    // ── Consumes order.cancel.requested → inventory alert (out of stock) ─────
    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0),
                    autoCreateTopics = "false")
    @KafkaListener(topics = "order.cancel.requested", groupId = "notification-service-cancellation")
    public void onOrderCancelRequested(@Payload String payload,
                                       @Header(value = "X-Trace-Id", required = false) String traceId,
                                       Acknowledgment ack) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId    = str(p, "orderId");
            String eventId    = str(event, "eventId");
            String traceIdVal = traceId != null ? traceId : str(event, "traceId");
            @SuppressWarnings("unchecked")
            var failedProducts = (List<String>) p.get("failedProducts");
            String failedProd  = failedProducts != null && !failedProducts.isEmpty()
                                 ? failedProducts.get(0) : "unknown";

            String email = RecipientResolver.resolveEmail(str(p, "customerEmail"), orderId);
            service.sendInventoryAlert(orderId, "unknown", email,
                    failedProd, traceIdVal, eventId);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed processing order.cancel.requested: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    // ── Consumes order.returned → send return confirmation ───────────────────
    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0),
                    autoCreateTopics = "false")
    @KafkaListener(topics = "order.returned", groupId = "notification-service-returns")
    public void onOrderReturned(@Payload String payload,
                                @Header(value = "X-Trace-Id", required = false) String traceId,
                                Acknowledgment ack) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId     = str(p, "orderId");
            String customerId  = str(p, "customerId");
            String eventId     = str(event, "eventId");
            String traceIdVal  = traceId != null ? traceId : str(event, "traceId");
            var    refundAmount = new java.math.BigDecimal(str(p, "refundAmount"));

            String email = RecipientResolver.resolveEmail(str(p, "customerEmail"), orderId);
            String phone = RecipientResolver.resolvePhone(str(p, "customerPhone"));
            service.sendReturnConfirmation(orderId, customerId,
                    email, phone, refundAmount, traceIdVal, eventId);
            ack.acknowledge();
        } catch (Exception ex) {
            log.error("Failed processing order.returned: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    // ── DLT handler — all exhausted retries land here and are persisted ──────
    @DltHandler
    public void handleDlt(@Payload String payload,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String dltTopic,
                          @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false) String originalTopic,
                          @Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Object originalPartition,
                          @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false) Object originalOffset,
                          @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String messageKey,
                          @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exceptionType,
                          @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exceptionMessage,
                          @Header(value = "X-Trace-Id", required = false) String traceId) {
        log.error("Message exhausted all retries and sent to DLT. topic={} partition={} offset={} exception={}",
                originalTopic, originalPartition, originalOffset, exceptionMessage);
        try {
            deadLetterRepository.save(new DeadLetterNotification(
                    originalTopic != null ? originalTopic : dltTopic,
                    toInteger(originalPartition),
                    toLong(originalOffset),
                    messageKey, payload, exceptionType, exceptionMessage, traceId));
        } catch (Exception ex) {
            // Last-resort: never let DLT persistence failure kill the listener
            log.error("Failed to persist dead-letter notification topic={}: {}", dltTopic, ex.getMessage(), ex);
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────
    @SuppressWarnings("unchecked")
    private Map<String, Object> parseEvent(String json) throws Exception {
        return mapper.readValue(json, Map.class);
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getPayload(Map<String, Object> event) {
        var p = event.get("payload");
        return p instanceof Map ? (Map<String, Object>) p : event;
    }

    private String str(Map<String, Object> map, String key) {
        var v = map.get(key);
        return v != null ? v.toString() : "";
    }

    // DLT headers may arrive as Integer/Long or raw byte[] depending on serializer
    private static Integer toInteger(Object value) {
        if (value == null) return null;
        if (value instanceof Integer i) return i;
        if (value instanceof Number n) return n.intValue();
        if (value instanceof byte[] bytes) return Integer.valueOf(new String(bytes));
        return Integer.valueOf(value.toString());
    }

    private static Long toLong(Object value) {
        if (value == null) return null;
        if (value instanceof Long l) return l;
        if (value instanceof Number n) return n.longValue();
        if (value instanceof byte[] bytes) return Long.valueOf(new String(bytes));
        return Long.valueOf(value.toString());
    }
}
