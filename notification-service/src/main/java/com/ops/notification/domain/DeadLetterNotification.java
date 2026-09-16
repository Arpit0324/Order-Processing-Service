package com.ops.notification.domain;

import jakarta.persistence.*;
import java.time.Instant;
import java.util.UUID;

// ── Persisted copy of a Kafka message that exhausted all retries (DLT) ───────
@Entity
@Table(name = "dead_letter_notifications", indexes = {
    @Index(name = "idx_dlt_created", columnList = "created_at")
})
public class DeadLetterNotification {

    @Id
    @Column(nullable = false, updatable = false)
    private String id;

    @Column(name = "original_topic", nullable = false)
    private String originalTopic;

    @Column(name = "original_partition")
    private Integer originalPartition;

    @Column(name = "original_offset")
    private Long originalOffset;

    @Column(name = "message_key")
    private String messageKey;

    @Column(columnDefinition = "text")
    private String payload;

    @Column(name = "exception_type")
    private String exceptionType;

    @Column(name = "exception_message", columnDefinition = "text")
    private String exceptionMessage;

    @Column(name = "trace_id")
    private String traceId;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected DeadLetterNotification() {}

    public DeadLetterNotification(String originalTopic, Integer originalPartition,
                                  Long originalOffset, String messageKey, String payload,
                                  String exceptionType, String exceptionMessage, String traceId) {
        this.id                = UUID.randomUUID().toString();
        this.originalTopic     = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset    = originalOffset;
        this.messageKey        = messageKey;
        this.payload           = payload;
        this.exceptionType     = exceptionType;
        this.exceptionMessage  = exceptionMessage;
        this.traceId           = traceId;
        this.createdAt         = Instant.now();
    }

    // ── Getters ───────────────────────────────────────────────────────────────
    public String  getId()                { return id; }
    public String  getOriginalTopic()     { return originalTopic; }
    public Integer getOriginalPartition() { return originalPartition; }
    public Long    getOriginalOffset()    { return originalOffset; }
    public String  getMessageKey()        { return messageKey; }
    public String  getPayload()           { return payload; }
    public String  getExceptionType()     { return exceptionType; }
    public String  getExceptionMessage()  { return exceptionMessage; }
    public String  getTraceId()           { return traceId; }
    public Instant getCreatedAt()         { return createdAt; }
}
