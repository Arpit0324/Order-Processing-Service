package com.ops.gateway.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;

// ── Filter 3: Redis-based rate limiting ───────────────────────────────────────
// Sliding window counter per client IP.
// Key: ops:ratelimit:{clientIp}   TTL: 60s   Limit: 100 requests/min
// Returns 429 with Retry-After header when limit exceeded.
// Bypass: same paths as JWT filter (health, swagger)
@Component
public class RateLimitFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(RateLimitFilter.class);
    private static final int    LIMIT_PER_MINUTE = 100;
    private static final String KEY_PREFIX       = "ops:ratelimit:";

    private static final List<String> BYPASS_PATHS = List.of(
            "/swagger-ui", "/v3/api-docs", "/actuator/health", "/actuator/info"
    );

    private final ReactiveStringRedisTemplate redis;

    public RateLimitFilter(ReactiveStringRedisTemplate redis) {
        this.redis = redis;
    }

    @Override
    public int getOrder() { return -50; }

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        if (BYPASS_PATHS.stream().anyMatch(path::startsWith)) {
            return chain.filter(exchange);
        }

        String clientIp  = getClientIp(exchange);
        String redisKey  = KEY_PREFIX + clientIp;

        return redis.opsForValue()
                .increment(redisKey)
                .flatMap(count -> {
                    if (count == 1) {
                        // First request in window — set expiry
                        return redis.expire(redisKey, Duration.ofMinutes(1))
                                .thenReturn(count);
                    }
                    return Mono.just(count);
                })
                .flatMap(count -> {
                    if (count > LIMIT_PER_MINUTE) {
                        log.warn("Rate limit exceeded clientIp={} count={}", clientIp, count);
                        return tooManyRequests(exchange);
                    }
                    // Expose remaining quota as response header
                    exchange.getResponse().getHeaders()
                            .add("X-RateLimit-Remaining", String.valueOf(LIMIT_PER_MINUTE - count));
                    return chain.filter(exchange);
                })
                .onErrorResume(ex -> {
                    // Redis down → fail open (let request through; don't block on cache failure)
                    log.error("Redis rate-limit check failed, allowing request: {}", ex.getMessage());
                    return chain.filter(exchange);
                });
    }

    private Mono<Void> tooManyRequests(ServerWebExchange exchange) {
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        response.getHeaders().add("Retry-After", "60");
        String body = """
                {"error":{"code":"RATE_LIMIT_EXCEEDED","message":"Too many requests. Limit: %d/min."}}
                """.formatted(LIMIT_PER_MINUTE);
        var buffer = response.bufferFactory().wrap(body.getBytes());
        return response.writeWith(Mono.just(buffer));
    }

    private String getClientIp(ServerWebExchange exchange) {
        String xff = exchange.getRequest().getHeaders().getFirst("X-Forwarded-For");
        if (xff != null && !xff.isBlank()) {
            return xff.split(",")[0].trim();
        }
        var remoteAddress = exchange.getRequest().getRemoteAddress();
        return remoteAddress != null ? remoteAddress.getHostString() : "unknown";
    }
}
