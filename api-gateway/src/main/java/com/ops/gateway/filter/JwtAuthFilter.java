package com.ops.gateway.filter;

import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.security.Keys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.gateway.filter.GatewayFilterChain;
import org.springframework.cloud.gateway.filter.GlobalFilter;
import org.springframework.core.Ordered;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.security.KeyFactory;
import java.security.PublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.List;

// ── Filter 1 (runs first): JWT validation ────────────────────────────────────
// Rejects requests with missing/invalid/expired JWT before they reach downstream.
// Extracts sub → injects X-User-Id header so downstream services trust the identity.
// Uses RS256 (asymmetric) — public key only on gateway, private key on auth service.
//
// Bypass paths (no JWT required): /swagger-ui, /v3/api-docs, /actuator/health
@Component
public class JwtAuthFilter implements GlobalFilter, Ordered {

    private static final Logger log = LoggerFactory.getLogger(JwtAuthFilter.class);

    private static final List<String> BYPASS_PATHS = List.of(
            "/swagger-ui", "/v3/api-docs", "/actuator/health", "/actuator/info"
    );

    private final PublicKey publicKey;

    public JwtAuthFilter(@Value("${gateway.jwt.public-key}") String base64PublicKey) {
        this.publicKey = loadPublicKey(base64PublicKey);
    }

    @Override
    public int getOrder() { return -200; } // run before rate-limit and logging

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, GatewayFilterChain chain) {
        String path = exchange.getRequest().getPath().value();

        if (BYPASS_PATHS.stream().anyMatch(path::startsWith)) {
            return chain.filter(exchange);
        }

        String authHeader = exchange.getRequest().getHeaders().getFirst(HttpHeaders.AUTHORIZATION);

        if (authHeader == null || !authHeader.startsWith("Bearer ")) {
            log.warn("Missing or malformed Authorization header path={}", path);
            return unauthorized(exchange, "Missing Authorization header");
        }

        String token = authHeader.substring(7);

        try {
            Claims claims = Jwts.parser()
                    .verifyWith(publicKey)
                    .build()
                    .parseSignedClaims(token)
                    .getPayload();

            String userId = claims.getSubject();

            // Inject validated identity for downstream services — they must not
            // re-validate the token; they trust this header (internal network only)
            ServerWebExchange mutated = exchange.mutate()
                    .request(r -> r.headers(h -> {
                        h.set("X-User-Id", userId);
                        h.remove(HttpHeaders.AUTHORIZATION); // strip raw JWT from downstream
                    }))
                    .build();

            log.debug("JWT valid userId={} path={}", userId, path);
            return chain.filter(mutated);

        } catch (Exception ex) {
            log.warn("JWT validation failed path={}: {}", path, ex.getMessage());
            return unauthorized(exchange, "Invalid or expired token");
        }
    }

    private Mono<Void> unauthorized(ServerWebExchange exchange, String message) {
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.UNAUTHORIZED);
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);
        String body = """
                {"error":{"code":"UNAUTHORIZED","message":"%s"}}
                """.formatted(message);
        var buffer = response.bufferFactory().wrap(body.getBytes());
        return response.writeWith(Mono.just(buffer));
    }

    private static PublicKey loadPublicKey(String base64Key) {
        try {
            // Strip PEM headers if present
            String stripped = base64Key
                    .replace("-----BEGIN PUBLIC KEY-----", "")
                    .replace("-----END PUBLIC KEY-----", "")
                    .replaceAll("\\s", "");
            byte[] decoded = Base64.getDecoder().decode(stripped);
            var spec = new X509EncodedKeySpec(decoded);
            return KeyFactory.getInstance("RSA").generatePublic(spec);
        } catch (Exception ex) {
            throw new IllegalStateException("Failed to load JWT public key: " + ex.getMessage(), ex);
        }
    }
}
