package com.ops.gateway.exception;

import java.time.Instant;

// ── Consistent error response envelope for all gateway errors ────────────────
// Used by GlobalErrorWebExceptionHandler, ErrorResponseFilter, and FallbackController.
public record ErrorResponse(
        String error,
        String message,
        String traceId,
        String timestamp,
        int status
) {
    public static ErrorResponse of(GatewayErrorCode code, String traceId) {
        return new ErrorResponse(
                code.name(),
                code.defaultMessage(),
                traceId,
                Instant.now().toString(),
                code.status().value()
        );
    }

    public static ErrorResponse of(GatewayErrorCode code, String message, String traceId) {
        return new ErrorResponse(
                code.name(),
                message,
                traceId,
                Instant.now().toString(),
                code.status().value()
        );
    }
}
