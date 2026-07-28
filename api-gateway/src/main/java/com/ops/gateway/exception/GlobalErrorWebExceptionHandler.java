package com.ops.gateway.exception;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.web.reactive.error.ErrorWebExceptionHandler;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;

import java.net.ConnectException;
import java.util.concurrent.TimeoutException;

// ── Global exception handler — catches ALL unhandled errors from the filter chain ─
// Maps known exception types to appropriate HTTP status codes and returns the
// standard ErrorResponse envelope. Runs at @Order(-1) to take precedence over
// Spring Boot's default error handler.
@Component
@Order(-1)
public class GlobalErrorWebExceptionHandler implements ErrorWebExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalErrorWebExceptionHandler.class);

    private final ObjectMapper objectMapper;

    public GlobalErrorWebExceptionHandler(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Mono<Void> handle(ServerWebExchange exchange, Throwable ex) {
        if (exchange.getResponse().isCommitted()) {
            return Mono.error(ex);
        }

        String traceId = exchange.getRequest().getHeaders().getFirst("X-Trace-Id");
        GatewayErrorCode errorCode = mapException(ex);

        log.error("Unhandled exception traceId={} error={}: {}",
                traceId, errorCode.name(), ex.getMessage(), ex);

        ErrorResponse errorResponse = ErrorResponse.of(errorCode, resolveMessage(ex, errorCode), traceId);
        return writeResponse(exchange, errorResponse);
    }

    private GatewayErrorCode mapException(Throwable ex) {
        if (ex instanceof ConnectException) {
            return GatewayErrorCode.BAD_GATEWAY;
        }
        if (ex instanceof TimeoutException) {
            return GatewayErrorCode.GATEWAY_TIMEOUT;
        }
        if (ex instanceof ResponseStatusException rse) {
            return mapStatusCode(rse.getStatusCode().value());
        }
        // Check cause chain for connection/timeout errors
        Throwable cause = ex.getCause();
        if (cause != null) {
            if (cause instanceof ConnectException) {
                return GatewayErrorCode.BAD_GATEWAY;
            }
            if (cause instanceof TimeoutException) {
                return GatewayErrorCode.GATEWAY_TIMEOUT;
            }
        }
        return GatewayErrorCode.INTERNAL_ERROR;
    }

    private GatewayErrorCode mapStatusCode(int statusCode) {
        return switch (statusCode) {
            case 400 -> GatewayErrorCode.BAD_REQUEST;
            case 401 -> GatewayErrorCode.UNAUTHORIZED;
            case 404 -> GatewayErrorCode.BAD_REQUEST;
            case 413 -> GatewayErrorCode.PAYLOAD_TOO_LARGE;
            case 429 -> GatewayErrorCode.RATE_LIMIT_EXCEEDED;
            case 502 -> GatewayErrorCode.BAD_GATEWAY;
            case 503 -> GatewayErrorCode.SERVICE_UNAVAILABLE;
            case 504 -> GatewayErrorCode.GATEWAY_TIMEOUT;
            default  -> GatewayErrorCode.INTERNAL_ERROR;
        };
    }

    private String resolveMessage(Throwable ex, GatewayErrorCode errorCode) {
        if (ex instanceof ResponseStatusException rse && rse.getReason() != null) {
            return rse.getReason();
        }
        return errorCode.defaultMessage();
    }

    private Mono<Void> writeResponse(ServerWebExchange exchange, ErrorResponse errorResponse) {
        var response = exchange.getResponse();
        response.setStatusCode(HttpStatus.valueOf(errorResponse.status()));
        response.getHeaders().setContentType(MediaType.APPLICATION_JSON);

        try {
            byte[] bytes = objectMapper.writeValueAsBytes(errorResponse);
            var buffer = response.bufferFactory().wrap(bytes);
            return response.writeWith(Mono.just(buffer));
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize error response", e);
            byte[] fallback = """
                    {"error":"INTERNAL_ERROR","message":"An unexpected error occurred","status":500}
                    """.getBytes();
            var buffer = response.bufferFactory().wrap(fallback);
            return response.writeWith(Mono.just(buffer));
        }
    }
}
