package com.ops.notification.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.time.Instant;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotificationConsumerTest {

    @Mock
    private NotificationService service;

    private ObjectMapper mapper;
    private NotificationConsumer consumer;

    private NotificationResponse dummyResponse(String orderId) {
        return new NotificationResponse("id", orderId, "EMAIL", "e@e.com",
                "ORDER_CONFIRMED", "PENDING", 0, null, null, Instant.now());
    }

    @BeforeEach
    void setUp() {
        mapper = new ObjectMapper().registerModule(new JavaTimeModule());
        consumer = new NotificationConsumer(service, mapper);
    }

    // ─── onOrderCreated ───────────────────────────────────────────────────────

    @Test
    void onOrderCreated_validPayload_shouldCallSendOrderConfirmation() throws Exception {
        when(service.sendOrderConfirmation(anyString(), anyString(), anyString(), any(), any(), anyString()))
                .thenReturn(dummyResponse("ord-1"));

        String payload = buildOrderCreatedPayload("ord-1", "cust-1", "99.99", "trace-abc");
        consumer.onOrderCreated(payload, "order.created", "trace-abc");

        verify(service).sendOrderConfirmation(
                eq("ord-1"), eq("cust-1"), anyString(), isNull(),
                eq(new BigDecimal("99.99")), eq("trace-abc"));
    }

    @Test
    void onOrderCreated_withoutHeaderTraceId_shouldFallbackToEventTraceId() throws Exception {
        when(service.sendOrderConfirmation(anyString(), anyString(), anyString(), any(), any(), anyString()))
                .thenReturn(dummyResponse("ord-2"));

        String payload = buildOrderCreatedPayload("ord-2", "cust-2", "10.00", "event-trace");
        consumer.onOrderCreated(payload, "order.created", null);

        ArgumentCaptor<String> traceCaptor = ArgumentCaptor.forClass(String.class);
        verify(service).sendOrderConfirmation(anyString(), anyString(), anyString(), any(),
                any(), traceCaptor.capture());
        assertThat(traceCaptor.getValue()).isEqualTo("event-trace");
    }

    @Test
    void onOrderCreated_invalidPayload_shouldThrowRuntimeException() {
        assertThatThrownBy(() -> consumer.onOrderCreated("{invalid-json}", "order.created", null))
                .isInstanceOf(RuntimeException.class);
    }

    // ─── onOrderCancelled ─────────────────────────────────────────────────────

    @Test
    void onOrderCancelled_validPayload_shouldCallSendOrderCancellation() throws Exception {
        when(service.sendOrderCancellation(anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(dummyResponse("ord-3"));

        String payload = buildOrderCancelledPayload("ord-3", "cust-3", "CUSTOMER_REQUEST", "trace-x");
        consumer.onOrderCancelled(payload, "trace-x");

        verify(service).sendOrderCancellation(
                eq("ord-3"), eq("cust-3"), anyString(), eq("CUSTOMER_REQUEST"), eq("trace-x"));
    }

    @Test
    void onOrderCancelled_invalidPayload_shouldThrowRuntimeException() {
        assertThatThrownBy(() -> consumer.onOrderCancelled("not-json", null))
                .isInstanceOf(RuntimeException.class);
    }

    // ─── onOrderCancelRequested ───────────────────────────────────────────────

    @Test
    void onOrderCancelRequested_withFailedProducts_shouldPassFirstProduct() throws Exception {
        when(service.sendInventoryAlert(anyString(), anyString(), anyString(), anyString(), anyString()))
                .thenReturn(dummyResponse("ord-4"));

        String payload = buildOrderCancelRequestedPayload("ord-4", "prod-A", "trace-y");
        consumer.onOrderCancelRequested(payload, "trace-y");

        verify(service).sendInventoryAlert(
                eq("ord-4"), anyString(), anyString(), eq("prod-A"), eq("trace-y"));
    }

    // ─── onOrderReturned ─────────────────────────────────────────────────────

    @Test
    void onOrderReturned_validPayload_shouldCallSendReturnConfirmation() throws Exception {
        when(service.sendReturnConfirmation(anyString(), anyString(), anyString(), any(), any(), anyString()))
                .thenReturn(dummyResponse("ord-5"));

        String payload = buildOrderReturnedPayload("ord-5", "cust-5", "50.00", "trace-z");
        consumer.onOrderReturned(payload, "trace-z");

        verify(service).sendReturnConfirmation(
                eq("ord-5"), eq("cust-5"), anyString(), isNull(),
                eq(new java.math.BigDecimal("50.00")), eq("trace-z"));
    }

    @Test
    void onOrderReturned_invalidPayload_shouldThrowRuntimeException() {
        assertThatThrownBy(() -> consumer.onOrderReturned("{bad-json}", null))
                .isInstanceOf(RuntimeException.class);
    }

    // ─── Helper builders ──────────────────────────────────────────────────────

    private String buildOrderCreatedPayload(String orderId, String customerId,
                                             String totalAmount, String traceId) throws Exception {
        return mapper.writeValueAsString(java.util.Map.of(
                "eventId",      "ev-1",
                "eventType",    "ORDER_CREATED",
                "eventVersion", 1,
                "occurredAt",   Instant.now().toString(),
                "traceId",      traceId,
                "payload", java.util.Map.of(
                        "orderId",     orderId,
                        "customerId",  customerId,
                        "totalAmount", totalAmount,
                        "items",       java.util.List.of()
                )
        ));
    }

    private String buildOrderCancelledPayload(String orderId, String customerId,
                                               String reason, String traceId) throws Exception {
        return mapper.writeValueAsString(java.util.Map.of(
                "eventId",      "ev-2",
                "eventType",    "ORDER_CANCELLED",
                "eventVersion", 1,
                "occurredAt",   Instant.now().toString(),
                "traceId",      traceId,
                "payload", java.util.Map.of(
                        "orderId",    orderId,
                        "customerId", customerId,
                        "reason",     reason
                )
        ));
    }

    private String buildOrderCancelRequestedPayload(String orderId, String failedProduct,
                                                     String traceId) throws Exception {
        return mapper.writeValueAsString(java.util.Map.of(
                "eventId",      "ev-3",
                "eventType",    "ORDER_CANCEL_REQUESTED",
                "eventVersion", 1,
                "occurredAt",   Instant.now().toString(),
                "traceId",      traceId,
                "payload", java.util.Map.of(
                        "orderId",        orderId,
                        "failedProducts", java.util.List.of(failedProduct)
                )
        ));
    }

    private String buildOrderReturnedPayload(String orderId, String customerId,
                                              String refundAmount, String traceId) throws Exception {
        return mapper.writeValueAsString(java.util.Map.of(
                "eventId",      "ev-4",
                "eventType",    "ORDER_RETURNED",
                "eventVersion", 1,
                "occurredAt",   Instant.now().toString(),
                "traceId",      traceId,
                "payload", java.util.Map.of(
                        "orderId",      orderId,
                        "customerId",   customerId,
                        "refundAmount", refundAmount
                )
        ));
    }
}
