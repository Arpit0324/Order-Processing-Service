package com.ops.notification.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ops.notification.consumer.NotificationConsumer;
import com.ops.notification.domain.NotificationRecord;
import com.ops.notification.kafka.NotificationEventProducer;
import com.ops.notification.repository.DeadLetterRepository;
import com.ops.notification.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// ── Contract tests: producer wire schema ↔ notification-service consumers ─────
// Canonical event fixtures live in src/test/resources/contracts and represent
// the agreed wire schema (envelope + payload, recipient snapshot). These tests
// fail if either side drifts from the versioned contract.
@ExtendWith(MockitoExtension.class)
class EventContractTest {

    @Mock private NotificationService  service;
    @Mock private DeadLetterRepository deadLetterRepository;
    @Mock private Acknowledgment       ack;

    private ObjectMapper          mapper;
    private NotificationConsumer  consumer;

    @BeforeEach
    void setUp() {
        mapper   = new ObjectMapper().registerModule(new JavaTimeModule());
        consumer = new NotificationConsumer(service, mapper, deadLetterRepository);
    }

    // ── Consumer side ────────────────────────────────────────────────────────

    @Test
    void orderCreatedContract_isConsumedWithRecipientSnapshot() throws Exception {
        consumer.onOrderCreated(fixture("order.created.v1.json"), "order.created", null, ack);

        verify(service).sendOrderConfirmation(
                eq("ord-1001"), eq("cust-1"),
                eq("buyer@example.com"), eq("+15551234567"),
                eq(new BigDecimal("39.98")), eq("trace-oc-001"), eq("evt-oc-001"));
        verify(ack).acknowledge();
    }

    @Test
    void orderCancelledContract_isConsumed() throws Exception {
        consumer.onOrderCancelled(fixture("order.cancelled.v1.json"), null, ack);

        verify(service).sendOrderCancellation(
                eq("ord-1002"), eq("cust-2"), eq("cancel@example.com"),
                eq("CUSTOMER_REQUEST"), eq("trace-ocx-002"), eq("evt-ocx-002"));
        verify(ack).acknowledge();
    }

    @Test
    void orderCancelRequestedContract_usesFirstFailedProduct() throws Exception {
        consumer.onOrderCancelRequested(fixture("order.cancel.requested.v1.json"), null, ack);

        verify(service).sendInventoryAlert(
                eq("ord-1003"), anyString(), eq("stockout@example.com"),
                eq("prod-9"), eq("trace-ocr-003"), eq("evt-ocr-003"));
        verify(ack).acknowledge();
    }

    @Test
    void orderReturnedContract_isConsumed() throws Exception {
        consumer.onOrderReturned(fixture("order.returned.v1.json"), null, ack);

        verify(service).sendReturnConfirmation(
                eq("ord-1004"), eq("cust-4"),
                eq("return@example.com"), eq("+15559876543"),
                eq(new BigDecimal("50.0")), eq("trace-ret-004"), eq("evt-ret-004"));
        verify(ack).acknowledge();
    }

    // ── Producer side (notif.sent) ───────────────────────────────────────────

    @Test
    @SuppressWarnings("unchecked")
    void notificationSentContract_matchesVersionedSchema() throws Exception {
        KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
        ArgumentCaptor<String> json = ArgumentCaptor.forClass(String.class);
        CompletableFuture<SendResult<String, String>> future =
                CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), json.capture())).thenReturn(future);

        var producer = new NotificationEventProducer(kafkaTemplate, mapper);
        var record   = new NotificationRecord("ord-1005", "EMAIL", "sent@example.com",
                "ORDER_CONFIRMED", "{}");
        producer.publishNotificationSent(record, "trace-ns-005");

        Map<String, Object> produced = mapper.readValue(json.getValue(), Map.class);
        Map<String, Object> expected = mapper.readValue(fixture("notif.sent.v1.json"), Map.class);

        // Envelope and payload key sets must match the versioned contract exactly.
        assertThat(produced.keySet()).isEqualTo(expected.keySet());
        assertThat(((Map<String, Object>) produced.get("payload")).keySet())
                .isEqualTo(((Map<String, Object>) expected.get("payload")).keySet());

        assertThat(produced.get("eventType")).isEqualTo("NOTIFICATION_SENT");
        assertThat(produced.get("eventVersion")).isEqualTo(1);
        assertThat(produced.get("traceId")).isEqualTo("trace-ns-005");

        Map<String, Object> payload = (Map<String, Object>) produced.get("payload");
        assertThat(payload.get("orderId")).isEqualTo("ord-1005");
        assertThat(payload.get("channel")).isEqualTo("EMAIL");
        assertThat(payload.get("recipient")).isEqualTo("sent@example.com");
    }

    // ── Every consumed fixture must carry the versioned envelope ─────────────
    @Test
    void allConsumedFixtures_carryEnvelopeContract() throws Exception {
        for (String name : Set.of("order.created.v1.json", "order.cancelled.v1.json",
                "order.cancel.requested.v1.json", "order.returned.v1.json")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> event = mapper.readValue(fixture(name), Map.class);
            assertThat(event).as("%s envelope", name)
                    .containsKeys("eventId", "eventType", "eventVersion", "occurredAt", "traceId", "payload");
            assertThat(event.get("eventVersion")).as("%s version", name).isEqualTo(1);
        }
    }

    private String fixture(String name) throws Exception {
        try (var in = getClass().getResourceAsStream("/contracts/" + name)) {
            assertThat(in).as("fixture %s must exist", name).isNotNull();
            return new String(in.readAllBytes(), StandardCharsets.UTF_8);
        }
    }
}
