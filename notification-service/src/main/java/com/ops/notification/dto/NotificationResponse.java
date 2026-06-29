package com.ops.notification.dto;

import com.ops.notification.domain.NotificationRecord;
import java.time.Instant;

public record NotificationResponse(
        String  id,
        String  orderId,
        String  channel,
        String  recipient,
        String  template,
        String  status,
        int     attempts,
        Instant sentAt,
        String  errorMsg,
        Instant createdAt
) {
    public static NotificationResponse from(NotificationRecord r) {
        return new NotificationResponse(
                r.getId(), r.getOrderId(), r.getChannel(), r.getRecipient(),
                r.getTemplate(), r.getStatus(), r.getAttempts(),
                r.getSentAt(), r.getErrorMsg(), r.getCreatedAt()
        );
    }
}
