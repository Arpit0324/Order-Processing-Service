package com.ops.notification.service;

import org.apache.pekko.actor.typed.ActorRef;
import com.ops.notification.actor.DeliveryResult;
import com.ops.notification.actor.NotificationCommand;
import com.ops.notification.actor.NotificationDispatcher;
import com.ops.notification.domain.NotificationRecord;
import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.kafka.NotificationEventProducer;
import com.ops.notification.repository.NotificationRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

// ── SERVICE LAYER: all business logic lives here ──────────────────────────────
// Consumer → Service → Dispatcher (actor ask) + Repository (persist) + Producer (Kafka)
//
// Reliability semantics:
//  - Duplicate events (same eventId) are no-ops — checked before insert and
//    enforced by a unique constraint for races.
//  - Dispatch is a synchronous ask: the Kafka offset is only acknowledged after
//    the channel actor reports success/failure.
//  - On failure we throw → @Transactional rolls back the PENDING row and
//    @RetryableTopic redelivers; after retries are exhausted the DLT handler
//    persists the message to dead_letter_notifications.
@Service
public class NotificationServiceImpl implements NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationServiceImpl.class);

    private final NotificationRepository      repository;
    private final NotificationDispatcher      dispatcher;
    private final NotificationEventProducer   producer;

    public NotificationServiceImpl(NotificationRepository repository,
                                   NotificationDispatcher dispatcher,
                                   NotificationEventProducer producer) {
        this.repository = repository;
        this.dispatcher = dispatcher;
        this.producer   = producer;
    }

    @Override
    @Transactional
    public NotificationResponse sendOrderConfirmation(String orderId, String customerId,
                                                       String email, String phone,
                                                       BigDecimal totalAmount, String traceId,
                                                       String eventId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CONFIRMED",
                "{\"totalAmount\":\"" + totalAmount + "\"}", eventId);
        persistAndDispatch(record,
                replyTo -> new NotificationCommand.SendOrderConfirmation(
                        record.getId(), orderId, customerId, email, phone, totalAmount, traceId, replyTo),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendOrderCancellation(String orderId, String customerId,
                                                       String email, String reason, String traceId,
                                                       String eventId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CANCELLED",
                "{\"reason\":\"" + reason + "\"}", eventId);
        persistAndDispatch(record,
                replyTo -> new NotificationCommand.SendOrderCancellation(
                        record.getId(), orderId, customerId, email, reason, traceId, replyTo),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendInventoryAlert(String orderId, String customerId,
                                                    String email, String failedProductId, String traceId,
                                                    String eventId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "ORDER_CANCELLED_OUT_OF_STOCK",
                "{\"productId\":\"" + failedProductId + "\"}", eventId);
        persistAndDispatch(record,
                replyTo -> new NotificationCommand.SendInventoryAlert(
                        record.getId(), orderId, customerId, email, failedProductId, traceId, replyTo),
                traceId);
        return NotificationResponse.from(record);
    }

    @Override
    @Transactional
    public NotificationResponse sendReturnConfirmation(String orderId, String customerId,
                                                        String email, String phone,
                                                        BigDecimal refundAmount, String traceId,
                                                        String eventId) {
        var record = new NotificationRecord(orderId, "EMAIL", email, "RETURN_APPROVED",
                "{\"refundAmount\":\"" + refundAmount + "\"}", eventId);
        persistAndDispatch(record,
                replyTo -> new NotificationCommand.SendReturnConfirmation(
                        record.getId(), orderId, customerId, email, phone, refundAmount, traceId, replyTo),
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

    // ── Private: dedupe → persist → synchronous dispatch → status transition ──
    private void persistAndDispatch(NotificationRecord record,
                                    Function<ActorRef<DeliveryResult>, NotificationCommand> commandFactory,
                                    String traceId) {
        // Idempotency: duplicate Kafka delivery of the same event is a no-op
        if (record.getEventId() != null
                && repository.existsByEventIdAndTemplateAndChannel(
                        record.getEventId(), record.getTemplate(), record.getChannel())) {
            log.info("Duplicate event skipped eventId={} template={} orderId={}",
                    record.getEventId(), record.getTemplate(), record.getOrderId());
            return;
        }

        try {
            repository.save(record);
        } catch (DataIntegrityViolationException dup) {
            // Race: another instance persisted the same event first
            log.info("Duplicate event (unique constraint) eventId={} template={} orderId={}",
                    record.getEventId(), record.getTemplate(), record.getOrderId());
            return;
        }

        DeliveryResult result = dispatcher.dispatch(commandFactory);

        if (result.success()) {
            record.markDelivered();
            repository.save(record);
            producer.publishNotificationSent(record, traceId);
            log.info("Notification delivered id={} template={} orderId={} traceId={}",
                    record.getId(), record.getTemplate(), record.getOrderId(), traceId);
        } else {
            // Roll back the PENDING row; Kafka redelivery retries the whole flow
            throw new NotificationDeliveryException(
                    "Delivery failed for notification " + record.getId() + ": " + result.error());
        }
    }
}
