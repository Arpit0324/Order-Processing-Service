package com.ops.notification.repository;

import com.ops.notification.domain.DeadLetterNotification;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface DeadLetterRepository extends JpaRepository<DeadLetterNotification, String> {
}
