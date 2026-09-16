package com.ops.notification.service;

// ── Thrown when notification delivery fails after persistence ────────────────
// Propagates to the Kafka consumer, which rethrows to trigger @RetryableTopic.
public class NotificationDeliveryException extends RuntimeException {

    public NotificationDeliveryException(String message) {
        super(message);
    }

    public NotificationDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
