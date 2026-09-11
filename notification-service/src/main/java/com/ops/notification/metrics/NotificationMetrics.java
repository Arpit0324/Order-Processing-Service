package com.ops.notification.metrics;

import com.ops.notification.repository.DeadLetterRepository;
import com.ops.notification.repository.NotificationRepository;
import io.micrometer.core.instrument.MeterRegistry;
import jakarta.annotation.PostConstruct;
import org.springframework.stereotype.Component;

// ── Backlog gauges for delivery health ───────────────────────────────────────
// Delivery success/failure counters live in NotificationRouter; these gauges
// expose the persisted backlog (pending, failed, dead-lettered) so Prometheus
// alerts and HPA/KEDA scaling can react to a growing tail of undelivered work.
@Component
public class NotificationMetrics {

    private final MeterRegistry          registry;
    private final NotificationRepository notifications;
    private final DeadLetterRepository   deadLetters;

    public NotificationMetrics(MeterRegistry registry,
                               NotificationRepository notifications,
                               DeadLetterRepository deadLetters) {
        this.registry      = registry;
        this.notifications = notifications;
        this.deadLetters   = deadLetters;
    }

    @PostConstruct
    void bindGauges() {
        registry.gauge("notification.records.pending", this,
                m -> m.notifications.countByStatus("PENDING"));
        registry.gauge("notification.records.failed", this,
                m -> m.notifications.countByStatus("FAILED"));
        registry.gauge("notification.deadletter.count", this,
                m -> m.deadLetters.count());
    }
}
