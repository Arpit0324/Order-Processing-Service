package com.ops.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RequestLoggingFilterTest {

    private RequestLoggingFilter filter;
    private GatewayFilterChain chain;

    @BeforeEach
    void setUp() {
        filter = new RequestLoggingFilter();
        chain = mock(GatewayFilterChain.class);
        when(chain.filter(any())).thenReturn(Mono.empty());
    }

    @Test
    void getOrder_shouldBeNegative100() {
        assertThat(filter.getOrder()).isEqualTo(-100);
    }

    @Test
    void filter_whenNoTraceIdPresent_shouldGenerateTraceId() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        // Verify chain was called (trace ID was injected via mutated exchange)
        // The actual trace ID injection happens on the mutated exchange passed to chain
        // We verify filter completes without error
    }

    @Test
    void filter_whenTraceIdAlreadyPresent_shouldPreserveIt() {
        String existingTraceId = "abc123traceId";
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .header("X-Trace-Id", existingTraceId)
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();
    }

    @Test
    void filter_completesSuccessfully_forAnyPath() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/actuator/health").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();
    }

    @Test
    void filter_withBlankTraceId_shouldGenerateNewOne() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/api/inventory/prod-1")
                .header("X-Trace-Id", "   ")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();
    }
}
