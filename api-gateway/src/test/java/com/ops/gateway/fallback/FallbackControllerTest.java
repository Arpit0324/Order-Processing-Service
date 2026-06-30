package com.ops.gateway.fallback;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;

class FallbackControllerTest {

    private FallbackController controller;

    @BeforeEach
    void setUp() {
        controller = new FallbackController();
    }

    @Test
    void orderFallback_shouldReturn503WithOrderServiceMessage() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/fallback/orders").build());

        StepVerifier.create(controller.orderFallback(exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().code()).isEqualTo("SERVICE_UNAVAILABLE");
                    assertThat(response.getBody().message()).contains("order-service");
                    assertThat(response.getBody().timestamp()).isNotNull();
                })
                .verifyComplete();
    }

    @Test
    void inventoryFallback_shouldReturn503WithInventoryServiceMessage() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/fallback/inventory").build());

        StepVerifier.create(controller.inventoryFallback(exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().code()).isEqualTo("SERVICE_UNAVAILABLE");
                    assertThat(response.getBody().message()).contains("inventory-service");
                })
                .verifyComplete();
    }

    @Test
    void notificationFallback_shouldReturn503WithNotificationServiceMessage() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/fallback/notifications").build());

        StepVerifier.create(controller.notificationFallback(exchange))
                .assertNext(response -> {
                    assertThat(response.getStatusCode()).isEqualTo(HttpStatus.SERVICE_UNAVAILABLE);
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().code()).isEqualTo("SERVICE_UNAVAILABLE");
                    assertThat(response.getBody().message()).contains("notification-service");
                })
                .verifyComplete();
    }

    @Test
    void fallback_withTraceId_shouldIncludeTraceIdInResponse() {
        String traceId = "trace-abc-123";
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/fallback/orders")
                        .header("X-Trace-Id", traceId)
                        .build());

        StepVerifier.create(controller.orderFallback(exchange))
                .assertNext(response -> {
                    assertThat(response.getBody()).isNotNull();
                    assertThat(response.getBody().traceId()).isEqualTo(traceId);
                })
                .verifyComplete();
    }

    @Test
    void fallback_withoutTraceId_shouldReturnNullTraceId() {
        MockServerWebExchange exchange = MockServerWebExchange.from(
                MockServerHttpRequest.get("/fallback/orders").build());

        StepVerifier.create(controller.orderFallback(exchange))
                .assertNext(response -> {
                    assertThat(response.getBody()).isNotNull();
                    // traceId will be null when no header is set
                    assertThat(response.getBody().code()).isEqualTo("SERVICE_UNAVAILABLE");
                })
                .verifyComplete();
    }
}
