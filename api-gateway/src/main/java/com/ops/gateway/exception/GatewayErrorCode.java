package com.ops.gateway.exception;

import org.springframework.http.HttpStatus;

// ── Standard error codes for all gateway-originated error responses ───────────
public enum GatewayErrorCode {

    UNAUTHORIZED(HttpStatus.UNAUTHORIZED, "Missing or invalid authentication token"),
    RATE_LIMIT_EXCEEDED(HttpStatus.TOO_MANY_REQUESTS, "Too many requests"),
    BAD_REQUEST(HttpStatus.BAD_REQUEST, "Malformed or invalid request"),
    PAYLOAD_TOO_LARGE(HttpStatus.PAYLOAD_TOO_LARGE, "Request body exceeds maximum allowed size"),
    SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE, "Service is temporarily unavailable"),
    BAD_GATEWAY(HttpStatus.BAD_GATEWAY, "Downstream service returned an invalid response"),
    GATEWAY_TIMEOUT(HttpStatus.GATEWAY_TIMEOUT, "Downstream service did not respond in time"),
    INTERNAL_ERROR(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");

    private final HttpStatus status;
    private final String defaultMessage;

    GatewayErrorCode(HttpStatus status, String defaultMessage) {
        this.status = status;
        this.defaultMessage = defaultMessage;
    }

    public HttpStatus status() { return status; }

    public String defaultMessage() { return defaultMessage; }
}
