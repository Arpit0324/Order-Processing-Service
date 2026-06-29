package com.ops.notification.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

// ── JPA entity — the only DB-facing type in the notification service ──────────
// Lives in domain/ because it IS the core domain object here (no separate
// domain ↔ entity split needed — notification records have no complex behaviour)
@Entity
@Table(name = "notifications", indexes = {
    @Index(name = "idx_notifications_order",   columnList = "order_id"),
    @Index(name = "idx_notifications_pending",  columnList = "status, created_at")
})
public class NotificationRecord {

    @Id
    @Column(nullable = false, updatable = false)
    private String id;

    @Column(name = "order_id", nullable = false)
    private String orderId;

    @Column(nullable = false, length = 10)
    private String channel;      // EMAIL | SMS | PUSH

    @Column(nullable = false)
    private String recipient;

    @Column(nullable = false)
    private String template;

    @Column(columnDefinition = "jsonb")
    private String payload;      // serialized JSON of template variables

    @Column(nullable = false, length = 10)
    private String status;       // PENDING | DELIVERED | FAILED

    @Column(nullable = false)
    private int attempts = 0;

    @Column(name = "sent_at")
    private Instant sentAt;

    @Column(name = "error_msg")
    private String errorMsg;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected NotificationRecord() {}

    public NotificationRecord(String orderId, String channel, String recipient,
                               String template, String payload) {
        this.id        = UUID.randomUUID().toString();
        this.orderId   = orderId;
        this.channel   = channel;
        this.recipient = recipient;
        this.template  = template;
        this.payload   = payload;
        this.status    = "PENDING";
        this.createdAt = Instant.now();
    }

    // ── Getters ───────────────────────────────────────────────────────────────
    public String  getId()        { return id; }
    public String  getOrderId()   { return orderId; }
    public String  getChannel()   { return channel; }
    public String  getRecipient() { return recipient; }
    public String  getTemplate()  { return template; }
    public String  getPayload()   { return payload; }
    public String  getStatus()    { return status; }
    public int     getAttempts()  { return attempts; }
    public Instant getSentAt()    { return sentAt; }
    public String  getErrorMsg()  { return errorMsg; }
    public Instant getCreatedAt() { return createdAt; }

    // ── State transitions ─────────────────────────────────────────────────────
    public void markDelivered() {
        this.status   = "DELIVERED";
        this.sentAt   = Instant.now();
        this.attempts = this.attempts + 1;
    }

    public void markFailed(String errorMsg) {
        this.status   = "FAILED";
        this.errorMsg = errorMsg;
        this.attempts = this.attempts + 1;
    }
}
