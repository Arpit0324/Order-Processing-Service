package com.ops.notification.repository;

import com.ops.notification.domain.NotificationRecord;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface NotificationRepository extends JpaRepository<NotificationRecord, String> {

    List<NotificationRecord> findByOrderIdOrderByCreatedAtDesc(String orderId);

    List<NotificationRecord> findByStatus(String status);

    // Find all failed/pending for retry job
    @Query("SELECT n FROM NotificationRecord n WHERE n.status IN ('PENDING','FAILED') AND n.attempts < 3")
    List<NotificationRecord> findRetryable();

    long countByOrderIdAndTemplate(String orderId, String template);
}
