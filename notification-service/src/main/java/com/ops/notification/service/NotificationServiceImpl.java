package com.ops.notification.service;

import akka.actor.typed.ActorRef;
import com.ops.notification.actor.NotificationCommand;
import com.ops.notification.domain.NotificationRecord;
import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.kafka.NotificationEventProducer;
import com.ops.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

// ── SERVICE LAYER: all business logic lives here ──────────────────────────────
// Consumer → Service → Actor (async dispatch) + Repository (persist) + Producer (Kafka)
@Service
public class NotificationServiceImpl implements NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceImpl.class);

    private final NotificationRepository   repository;
    private final ActorRef<NotificationCommand> router;
    private final NotificationEventProducer    producer;

    public NotificationServiceImpl(NotificationRepository repository,
                                   ActorRef<NotificationCommand> router,
                                   NotificationEventProducer producer) {
        this.repository = repository;
        this.router     = router;
        this.producer   = producer;
    }

    @Override
    @Transactional
    public NotificationResponse sendOrderConfirmation(String orderId, String customerId,
                                                       String email, String phone,
                                                       BigDecimal totalAmount, String traceId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CONFIRMED",
                "{\"totalAmount\":\"" + totalAmount + "\"}");
        persistAndDispatch(record,
                new NotificationCommand.SendOrderConfirmation(orderId, customerId, email, phone, totalAmount, traceId),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendOrderCancellation(String orderId, String customerId,
                                                       String email, String reason, String traceId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CANCELLED",
                "{\"reason\":\"" + reason + "\"}");
        persistAndDispatch(record,
                new NotificationCommand.SendOrderCancellation(orderId, customerId, email, reason, traceId),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendInventoryAlert(String orderId, String customerId,
                                                    String email, String failedProductId, String traceId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CANCELLED_OUT_OF_STOCK",
                "{\"productId\":\"" + failedProductId + "\"}");
        persistAndDispatch(record,
                new NotificationCommand.SendInventoryAlert(orderId, customerId, email, failedProductId, traceId),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendReturnConfirmation(String orderId, String customerId,
                                                        String email, String phone,
                                                        BigDecimal refundAmount, String traceId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "RETURN_APPROVED",
                "{\"refundAmount\":\"" + refundAmount + "\"}");
        persistAndDispatch(record,
                new NotificationCommand.SendReturnConfirmation(orderId, customerId, email, phone, refundAmount, traceId),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    public Optional<NotificationResponse> getById(String id) {
        return repository.findById(id).map(NotificationResponse::from);
    }

    @Override
    public List<NotificationResponse> getByOrderId(String orderId) {
        return repository.findByOrderIdOrderByCreatedAtDesc(orderId)
                .stream().map(NotificationResponse::from).toList();
    }

    // ── Private: save to DB first, then fire-and-forget to actor ─────────────
    // DB save is synchronous (within @Transactional) — ensures audit record exists
    // even if actor dispatch fails. Kafka publish is async best-effort.
    private void persistAndDispatch(NotificationRecord record,
                                    NotificationCommand command,
                                    String traceId) {
        repository.save(record);

        // Dispatch to actor pool (non-blocking, fire-and-forget)
        router.tell(command);

        // Publish Kafka event asynchronously
        producer.publishNotificationSent(record, traceId);

        log.info("Notification dispatched id={} template={} orderId={} traceId={}",
                record.getId(), record.getTemplate(), record.getOrderId(), traceId);
    }
}
