package com.ops.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ops.notification.service.NotificationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

// ── ENDPOINT (Kafka entry point) — delegates immediately to service layer ─────
// @RetryableTopic: 3 retries with exponential backoff before sending to DLT.
// DLT = order.created.DLT — monitored via Kafka UI; alert on non-zero lag.
@Component
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final NotificationService service;
    private final ObjectMapper        mapper;

    public NotificationConsumer(NotificationService service, ObjectMapper mapper) {
        this.service = service;
        this.mapper  = mapper;
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
                               @Header(value = "X-Trace-Id", required = false) String traceId) {
        log.debug("Received order.created message traceId={}", traceId);
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p = getPayload(event);

            String orderId     = str(p, "orderId");
            String customerId  = str(p, "customerId");
            String traceIdVal  = traceId != null ? traceId : str(event, "traceId");

            // Extract contact info from customer (in a real system: call customer service)
            // For now, use a default — in production inject a CustomerClient here
            String email = orderId + "@placeholder.ops";  // TODO: CustomerClient.getEmail(customerId)
            String phone = null;

            @SuppressWarnings("unchecked")
            var items = (List<?>) p.get("items");
            var total = new java.math.BigDecimal(str(p, "totalAmount"));

            service.sendOrderConfirmation(orderId, customerId, email, phone, total, traceIdVal);
        } catch (Exception ex) {
            log.error("Failed processing order.created: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex); // trigger @RetryableTopic retry
        }
    }

    // ── Consumes order.cancelled → send cancellation notice ──────────────────
    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0),
                    autoCreateTopics = "false")
    @KafkaListener(topics = "order.cancelled", groupId = "notification-service-cancelled")
    public void onOrderCancelled(@Payload String payload,
                                 @Header(value = "X-Trace-Id", required = false) String traceId) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId    = str(p, "orderId");
            String customerId = str(p, "customerId");
            String reason     = str(p, "reason");
            String email      = orderId + "@placeholder.ops";  // TODO: CustomerClient
            String traceIdVal = traceId != null ? traceId : str(event, "traceId");

            service.sendOrderCancellation(orderId, customerId, email, reason, traceIdVal);
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
                                       @Header(value = "X-Trace-Id", required = false) String traceId) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId    = str(p, "orderId");
            String traceIdVal = traceId != null ? traceId : str(event, "traceId");
            @SuppressWarnings("unchecked")
            var failedProducts = (List<String>) p.get("failedProducts");
            String failedProd  = failedProducts != null && !failedProducts.isEmpty()
                                 ? failedProducts.get(0) : "unknown";

            // customerId would come from CustomerClient in production
            service.sendInventoryAlert(orderId, "unknown", orderId + "@placeholder.ops", failedProd, traceIdVal);
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
                                @Header(value = "X-Trace-Id", required = false) String traceId) {
        try {
            Map<String, Object> event = parseEvent(payload);
            Map<String, Object> p     = getPayload(event);

            String orderId     = str(p, "orderId");
            String customerId  = str(p, "customerId");
            String traceIdVal  = traceId != null ? traceId : str(event, "traceId");
            var    refundAmount = new java.math.BigDecimal(str(p, "refundAmount"));

            service.sendReturnConfirmation(orderId, customerId,
                    orderId + "@placeholder.ops", null, refundAmount, traceIdVal);
        } catch (Exception ex) {
            log.error("Failed processing order.returned: {}", ex.getMessage(), ex);
            throw new RuntimeException(ex);
        }
    }

    // ── DLT handler — all exhausted retries land here ────────────────────────
    @DltHandler
    public void handleDlt(@Payload String payload,
                          @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("Message exhausted all retries and sent to DLT. topic={} payload={}",
                topic, payload);
        // In production: alert PagerDuty / Slack, store to dead_letter_notifications table
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
}
