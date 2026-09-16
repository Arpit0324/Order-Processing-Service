package com.ops.notification.service;

import com.ops.notification.dto.NotificationResponse;
import java.util.List;
import java.util.Optional;
import java.math.*;

public interface NotificationService {
    NotificationResponse sendOrderConfirmation(String orderId, String customerId,
                                               String email, String phone,
                                               BigDecimal totalAmount, String traceId,
                                               String eventId);

    NotificationResponse sendOrderCancellation(String orderId, String customerId,
                                               String email, String reason, String traceId,
                                               String eventId);

    NotificationResponse sendInventoryAlert(String orderId, String customerId,
                                            String email, String failedProductId, String traceId,
                                            String eventId);

    NotificationResponse sendReturnConfirmation(String orderId, String customerId,
                                                String email, String phone,
                                                BigDecimal refundAmount, String traceId,
                                                String eventId);

    Optional<NotificationResponse> getById(String id);

    List<NotificationResponse> getByOrderId(String orderId);
}
