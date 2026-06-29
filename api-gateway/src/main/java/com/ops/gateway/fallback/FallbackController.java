package com.ops.gateway.fallback;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Instant;

// ── Fallback controller — called when a circuit breaker opens ─────────────────
// Returns a structured 503 response with the service name and traceId.
// Never blocks: fully reactive.
@RestController
public class FallbackController {

    private static final Logger log = LoggerFactory.getLogger(FallbackController.class);

    @RequestMapping("/fallback/orders")
    public Mono<ResponseEntity<FallbackResponse>> orderFallback(ServerWebExchange exchange) {
        return fallback(exchange, "order-service");
    }

    @RequestMapping("/fallback/inventory")
    public Mono<ResponseEntity<FallbackResponse>> inventoryFallback(ServerWebExchange exchange) {
        return fallback(exchange, "inventory-service");
    }

    @RequestMapping("/fallback/notifications")
    public Mono<ResponseEntity<FallbackResponse>> notificationFallback(ServerWebExchange exchange) {
        return fallback(exchange, "notification-service");
    }

    private Mono<ResponseEntity<FallbackResponse>> fallback(ServerWebExchange exchange, String service) {
        String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
        log.warn("Circuit breaker open for service={} traceId={}", service, traceId);
        return Mono.just(ResponseEntity
                .status(HttpStatus.SERVICE_UNAVAILABLE)
                .contentType(MediaType.APPLICATION_JSON)
                .body(new FallbackResponse(
                        "SERVICE_UNAVAILABLE",
                        service + " is temporarily unavailable. Please try again shortly.",
                        traceId,
                        Instant.now().toString()
                )));
    }

    public record FallbackResponse(String code, String message, String traceId, String timestamp) {}
}
