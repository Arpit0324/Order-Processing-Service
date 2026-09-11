package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import java.math.BigDecimal;

// ── Sealed interface — all messages the NotificationRouter actor accepts ──────
// Every command carries notificationId (the persisted DB record) and replyTo
// for explicit delivery acknowledgement. DeliveryAttempted is the internal
// adaptation message produced by the router's ask on channel actors.
public sealed interface NotificationCommand permits
        NotificationCommand.SendOrderConfirmation,
        NotificationCommand.SendOrderCancellation,
        NotificationCommand.SendInventoryAlert,
        NotificationCommand.SendReturnConfirmation,
        NotificationCommand.SendRefundInitiated,
        NotificationCommand.DeliveryAttempted {

    // ── Order confirmed ───────────────────────────────────────────────────────
    record SendOrderConfirmation(
            String notificationId,
            String orderId,
            String customerId,
            String email,
            String phone,
            BigDecimal totalAmount,
            String traceId,
            ActorRef<DeliveryResult> replyTo
    ) implements NotificationCommand {}

    // ── Order cancelled ───────────────────────────────────────────────────────
    record SendOrderCancellation(
            String notificationId,
            String orderId,
            String customerId,
            String email,
            String reason,
            String traceId,
            ActorRef<DeliveryResult> replyTo
    ) implements NotificationCommand {}

    // ── Inventory alert (out of stock) ────────────────────────────────────────
    record SendInventoryAlert(
            String notificationId,
            String orderId,
            String customerId,
            String email,
            String failedProductId,
            String traceId,
            ActorRef<DeliveryResult> replyTo
    ) implements NotificationCommand {}

    // ── Return approved ───────────────────────────────────────────────────────
    record SendReturnConfirmation(
            String notificationId,
            String orderId,
            String customerId,
            String email,
            String phone,
            BigDecimal refundAmount,
            String traceId,
            ActorRef<DeliveryResult> replyTo
    ) implements NotificationCommand {}

    // ── Refund initiated ──────────────────────────────────────────────────────
    record SendRefundInitiated(
            String notificationId,
            String orderId,
            String customerId,
            String email,
            BigDecimal refundAmount,
            String traceId,
            ActorRef<DeliveryResult> replyTo
    ) implements NotificationCommand {}

    // ── Internal: channel-actor ack adapted back into the router protocol ─────
    record DeliveryAttempted(
            String notificationId,
            ActorRef<DeliveryResult> replyTo,
            DeliveryAck ack,
            Throwable failure
    ) implements NotificationCommand {}
}
