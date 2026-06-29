package com.ops.notification.actor;

import java.math.BigDecimal;

// ── Sealed interface — all commands the NotificationRouter actor accepts ───────
public sealed interface NotificationCommand permits
        NotificationCommand.SendOrderConfirmation,
        NotificationCommand.SendOrderCancellation,
        NotificationCommand.SendInventoryAlert,
        NotificationCommand.SendReturnConfirmation,
        NotificationCommand.SendRefundInitiated {

    // ── Order confirmed ───────────────────────────────────────────────────────
    record SendOrderConfirmation(
            String orderId,
            String customerId,
            String email,
            String phone,
            BigDecimal totalAmount,
            String traceId
    ) implements NotificationCommand {}

    // ── Order cancelled ───────────────────────────────────────────────────────
    record SendOrderCancellation(
            String orderId,
            String customerId,
            String email,
            String reason,
            String traceId
    ) implements NotificationCommand {}

    // ── Inventory alert (out of stock) ────────────────────────────────────────
    record SendInventoryAlert(
            String orderId,
            String customerId,
            String email,
            String failedProductId,
            String traceId
    ) implements NotificationCommand {}

    // ── Return approved ───────────────────────────────────────────────────────
    record SendReturnConfirmation(
            String orderId,
            String customerId,
            String email,
            String phone,
            BigDecimal refundAmount,
            String traceId
    ) implements NotificationCommand {}

    // ── Refund initiated ──────────────────────────────────────────────────────
    record SendRefundInitiated(
            String orderId,
            String customerId,
            String email,
            BigDecimal refundAmount,
            String traceId
    ) implements NotificationCommand {}
}
