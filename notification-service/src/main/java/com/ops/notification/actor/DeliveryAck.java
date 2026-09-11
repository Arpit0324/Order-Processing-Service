package com.ops.notification.actor;

// ── Reply sent by channel actors (email/SMS) to the router after attempting delivery ──
public record DeliveryAck(boolean success, String error) {

    public static DeliveryAck ok() {
        return new DeliveryAck(true, null);
    }

    public static DeliveryAck failed(String error) {
        return new DeliveryAck(false, error);
    }
}
