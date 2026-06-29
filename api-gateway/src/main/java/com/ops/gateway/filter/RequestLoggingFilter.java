package com.ops.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

// ── Filter 2: Request logging + traceId injection ─────────────────────────────
// Generates X-Trace-Id (if not already set by client) and injects it into
// every downstream request header so all services share the same traceId.
// Logs: method, path, userId, traceId, and response time.
@Component
public class RequestLoggingFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RequestLoggingFilter.class);

    @Override
    public int getOrder() { return -100; }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
        if (traceId == null || traceId.isBlank()) {
            traceId = UUID.randomUUID().toString().replace("-", "");
        }

        final String finalTraceId = traceId;
        final long   startMs      = Instant.now().toEpochMilli();
        final String method       = exchange.getRequest().getMethod().name();
        final String path         = exchange.getRequest().getPath().value();
        final String userId       = exchange.getRequest().getHeaders().getFirst("X-User-Id");

        log.info("→ {} {} userId={} traceId={}", method, path, userId, finalTraceId);

        ServerWebExchange mutated = exchange.mutate()
                .request(r -> r.headers(h -> h.set("X-Trace-Id", finalTraceId)))
                .build();

        return chain.filter(mutated).doFinally(signal -> {
            long durationMs = Instant.now().toEpochMilli() - startMs;
            int  status     = mutated.getResponse().getStatusCode() != null
                              ? mutated.getResponse().getStatusCode().value() : 0;
            log.info("← {} {} status={} duration={}ms traceId={}", method, path, status, durationMs, finalTraceId);
        });
    }
}
