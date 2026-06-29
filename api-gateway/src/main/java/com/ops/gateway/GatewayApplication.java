package com.ops.gateway;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

// ── Boot sequence ─────────────────────────────────────────────────────────────
// 1. Spring Boot context starts (WebFlux — non-blocking event loop)
// 2. Redis connection established (rate-limit + idempotency)
// 3. Global filters registered in order:
//      JwtAuthFilter   (order -200) → validates RS256 JWT, injects X-User-Id
//      RequestLoggingFilter (order -100) → generates/injects X-Trace-Id, logs
//      RateLimitFilter (order -50)  → Redis INCR per IP, 100 req/min
// 4. Routes registered:
//      /api/orders/**        → order-service:8081 (CB: orderCircuitBreaker)
//      /api/inventory/**     → inventory-service:8082 (CB: inventoryCircuitBreaker)
//      /api/notifications/** → notification-service:8083 (CB: notificationCircuitBreaker)
// 5. Fallback endpoints registered: /fallback/orders, /fallback/inventory, /fallback/notifications
// 6. Swagger UI served at /swagger-ui.html
// 7. HTTP server starts on port 8080
@SpringBootApplication
public class GatewayApplication {
    public static void main(String[] args) {
        SpringApplication.run(GatewayApplication.class, args);
    }
}
