package com.ops.notification.service;

import com.ops.notification.dto.NotificationResponse;
import java.util.List;
import java.util.Optional;

public interface NotificationService {
    NotificationResponse sendOrderConfirmation(String orderId, String customerId,
                                               String email, String phone,
                                               java.math.BigDecimal totalAmount, String traceId);

    NotificationResponse sendOrderCancellation(String orderId, String customerId,
                                               String email, String reason, String traceId);

    NotificationResponse sendInventoryAlert(String orderId, String customerId,
                                            String email, String failedProductId, String traceId);

    NotificationResponse sendReturnConfirmation(String orderId, String customerId,
                                                String email, String phone,
                                                java.math.BigDecimal refundAmount, String traceId);

    Optional<NotificationResponse> getById(String id);

    List<NotificationResponse> getByOrderId(String orderId);
}
