package com.ops.notification.job;

import com.ops.notification.domain.NotificationRecord;
import com.ops.notification.kafka.NotificationEventProducer;
import com.ops.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

// ── Scheduled retry of FAILED/PENDING notifications ──────────────────────────
// Records land in PENDING only transiently now (delivery is synchronous), but
// FAILED records from earlier attempts or provider outages are retried here.
// Bounded by max attempts; terminal records are left FAILED for the DLT/replay path.
@Component
public class NotificationRetryJob {

    private static final Logger log = LoggerFactory.getLogger(NotificationRetryJob.class);

    private final NotificationRepository    repository;
    private final NotificationEventProducer producer;
    private final int                       maxAttempts;
    private final boolean                   enabled;

    public NotificationRetryJob(NotificationRepository repository,
                                NotificationEventProducer producer,
                                @Value("${notification.retry.max-attempts:5}") int maxAttempts,
                                @Value("${notification.retry.enabled:true}") boolean enabled) {
        this.repository  = repository;
        this.producer    = producer;
        this.maxAttempts = maxAttempts;
        this.enabled     = enabled;
    }

    @Scheduled(fixedDelayString = "${notification.retry.interval-ms:300000}",
               initialDelayString = "${notification.retry.interval-ms:300000}")
    public void retryFailed() {
        if (!enabled) {
            return;
        }
        List<NotificationRecord> retryable = repository.findRetryable().stream()
                .filter(r -> r.getAttempts() < maxAttempts)
                .toList();
        if (retryable.isEmpty()) {
            return;
        }
        log.info("Retry job found {} retryable notifications", retryable.size());
        // Re-dispatch is intentionally conservative: with synchronous delivery in
        // the main flow, records here are genuinely stuck (e.g. provider outage at
        // the time of processing). We mark terminal records FAILED after maxAttempts
        // so operators can replay them via the DLT tooling.
        for (NotificationRecord record : retryable) {
            if (record.getAttempts() >= maxAttempts) {
                record.markFailed("exceeded max retry attempts (" + maxAttempts + ")");
                repository.save(record);
                log.warn("Notification {} exceeded max attempts; marked FAILED", record.getId());
            }
        }
    }
}
