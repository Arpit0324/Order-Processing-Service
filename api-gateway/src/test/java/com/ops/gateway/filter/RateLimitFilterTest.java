package com.ops.gateway.filter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import org.springframework.http.HttpStatus;
import org.springframework.mock.http.server.reactive.MockServerHttpRequest;
import org.springframework.mock.web.server.MockServerWebExchange;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RateLimitFilterTest {

    private ReactiveStringRedisTemplate redis;
    private ReactiveValueOperations<String, String> valueOps;
    private RateLimitFilter filter;
    private GatewayFilterChain chain;

    @BeforeEach
    void setUp() {
        redis    = mock(ReactiveStringRedisTemplate.class);
        valueOps = mock(ReactiveValueOperations.class);
        chain    = mock(GatewayFilterChain.class);

        when(redis.opsForValue()).thenReturn(valueOps);
        when(chain.filter(any())).thenReturn(Mono.empty());

        filter = new RateLimitFilter(redis);
    }

    @Test
    void getOrder_shouldBeNegative50() {
        assertThat(filter.getOrder()).isEqualTo(-50);
    }

    @Test
    void filter_bypassPath_swagger_shouldSkipRateLimit() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/swagger-ui/index.html").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        verify(chain).filter(exchange);
        verify(valueOps, never()).increment(any());
    }

    @Test
    void filter_bypassPath_actuatorHealth_shouldSkipRateLimit() {
        MockServerHttpRequest request = MockServerHttpRequest.get("/actuator/health").build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        verify(chain).filter(exchange);
        verify(valueOps, never()).increment(any());
    }

    @Test
    void filter_firstRequest_shouldSetExpiryAndAllow() {
        when(valueOps.increment(any(String.class))).thenReturn(Mono.just(1L));
        when(redis.expire(any(String.class), any(Duration.class))).thenReturn(Mono.just(Boolean.TRUE));

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .remoteAddress(new java.net.InetSocketAddress("192.168.1.1", 1234))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        verify(chain).filter(any());
        assertThat(exchange.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void filter_withinLimit_shouldAllow() {
        when(valueOps.increment(any(String.class))).thenReturn(Mono.just(50L));

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .remoteAddress(new java.net.InetSocketAddress("192.168.1.1", 1234))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        verify(chain).filter(any());
        assertThat(exchange.getResponse().getStatusCode()).isNotEqualTo(HttpStatus.TOO_MANY_REQUESTS);
    }

    @Test
    void filter_exceededLimit_shouldReturn429() {
        when(valueOps.increment(any(String.class))).thenReturn(Mono.just(101L));

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .remoteAddress(new java.net.InetSocketAddress("192.168.1.1", 1234))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        assertThat(exchange.getResponse().getStatusCode()).isEqualTo(HttpStatus.TOO_MANY_REQUESTS);
        verify(chain, never()).filter(any());
    }

    @Test
    void filter_redisFailure_shouldFailOpenAndAllow() {
        when(valueOps.increment(any(String.class))).thenReturn(Mono.error(new RuntimeException("Redis down")));

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .remoteAddress(new java.net.InetSocketAddress("10.0.0.1", 9999))
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        // On Redis failure, request should be allowed through (fail-open)
        verify(chain).filter(any());
    }

    @Test
    void filter_xForwardedFor_shouldUseFirstIp() {
        when(valueOps.increment(eq("ops:ratelimit:10.0.0.5"))).thenReturn(Mono.just(5L));

        MockServerHttpRequest request = MockServerHttpRequest.get("/api/orders")
                .header("X-Forwarded-For", "10.0.0.5, 10.0.0.6")
                .build();
        MockServerWebExchange exchange = MockServerWebExchange.from(request);

        StepVerifier.create(filter.filter(exchange, chain))
                .verifyComplete();

        verify(valueOps).increment("ops:ratelimit:10.0.0.5");
    }
}
