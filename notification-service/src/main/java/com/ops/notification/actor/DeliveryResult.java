package com.ops.notification.actor;

// ── Reply sent by the router back to the service layer for one notification ──
public record DeliveryResult(String notificationId, boolean success, String error) {

    public static DeliveryResult ok(String notificationId) {
        return new DeliveryResult(notificationId, true, null);
    }

    public static DeliveryResult failed(String notificationId, String error) {
        return new DeliveryResult(notificationId, false, error);
    }
}
