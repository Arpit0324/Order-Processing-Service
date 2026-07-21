package com.ops.notification.service;

import org.apache.pekko.actor.typed.ActorRef;
import com.ops.notification.actor.NotificationCommand;
import com.ops.notification.domain.NotificationRecord;
import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.kafka.NotificationEventProducer;
import com.ops.notification.repository.NotificationRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotificationServiceImplTest {

    @Mock
    private NotificationRepository repository;

    @Mock
    @SuppressWarnings("unchecked")
    private ActorRef<NotificationCommand> router;

    @Mock
    private NotificationEventProducer producer;

    private NotificationServiceImpl service;

    @BeforeEach
    void setUp() {
        service = new NotificationServiceImpl(repository, router, producer);
    }

    // ─── sendOrderConfirmation ────────────────────────────────────────────────

    @Test
    void sendOrderConfirmation_shouldPersistRecordWithCorrectFields() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        NotificationResponse response = service.sendOrderConfirmation(
                "ord-1", "cust-1", "test@example.com", "+1234567890",
                new BigDecimal("99.99"), "trace-1");

        ArgumentCaptor<NotificationRecord> captor = ArgumentCaptor.forClass(NotificationRecord.class);
        verify(repository).save(captor.capture());

        NotificationRecord saved = captor.getValue();
        assertThat(saved.getOrderId()).isEqualTo("ord-1");
        assertThat(saved.getChannel()).isEqualTo("EMAIL");
        assertThat(saved.getRecipient()).isEqualTo("test@example.com");
        assertThat(saved.getTemplate()).isEqualTo("ORDER_CONFIRMED");
        assertThat(saved.getStatus()).isEqualTo("PENDING");
    }

    @Test
    void sendOrderConfirmation_shouldDispatchToActorRouter() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendOrderConfirmation("ord-1", "cust-1", "test@example.com", null,
                new BigDecimal("99.99"), "trace-1");

        ArgumentCaptor<NotificationCommand> captor = ArgumentCaptor.forClass(NotificationCommand.class);
        verify(router).tell(captor.capture());

        assertThat(captor.getValue()).isInstanceOf(NotificationCommand.SendOrderConfirmation.class);
        NotificationCommand.SendOrderConfirmation cmd =
                (NotificationCommand.SendOrderConfirmation) captor.getValue();
        assertThat(cmd.orderId()).isEqualTo("ord-1");
        assertThat(cmd.traceId()).isEqualTo("trace-1");
    }

    @Test
    void sendOrderConfirmation_shouldPublishKafkaEvent() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendOrderConfirmation("ord-1", "cust-1", "test@example.com", null,
                new BigDecimal("99.99"), "trace-1");

        verify(producer).publishNotificationSent(any(NotificationRecord.class), eq("trace-1"));
    }

    @Test
    void sendOrderConfirmation_shouldReturnResponseWithPendingStatus() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        NotificationResponse response = service.sendOrderConfirmation(
                "ord-1", "cust-1", "test@example.com", null,
                new BigDecimal("49.50"), "trace-1");

        assertThat(response).isNotNull();
        assertThat(response.orderId()).isEqualTo("ord-1");
        assertThat(response.status()).isEqualTo("PENDING");
        assertThat(response.template()).isEqualTo("ORDER_CONFIRMED");
    }

    // ─── sendOrderCancellation ────────────────────────────────────────────────

    @Test
    void sendOrderCancellation_shouldPersistWithOrderCancelledTemplate() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendOrderCancellation("ord-2", "cust-2", "cancel@example.com",
                "CUSTOMER_REQUEST", "trace-2");

        ArgumentCaptor<NotificationRecord> captor = ArgumentCaptor.forClass(NotificationRecord.class);
        verify(repository).save(captor.capture());

        assertThat(captor.getValue().getTemplate()).isEqualTo("ORDER_CANCELLED");
        assertThat(captor.getValue().getOrderId()).isEqualTo("ord-2");
    }

    @Test
    void sendOrderCancellation_shouldDispatchSendOrderCancellationCommand() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendOrderCancellation("ord-2", "cust-2", "cancel@example.com",
                "CUSTOMER_REQUEST", "trace-2");

        ArgumentCaptor<NotificationCommand> captor = ArgumentCaptor.forClass(NotificationCommand.class);
        verify(router).tell(captor.capture());

        assertThat(captor.getValue()).isInstanceOf(NotificationCommand.SendOrderCancellation.class);
    }

    // ─── sendInventoryAlert ───────────────────────────────────────────────────

    @Test
    void sendInventoryAlert_shouldPersistWithOutOfStockTemplate() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendInventoryAlert("ord-3", "cust-3", "alert@example.com",
                "prod-99", "trace-3");

        ArgumentCaptor<NotificationRecord> captor = ArgumentCaptor.forClass(NotificationRecord.class);
        verify(repository).save(captor.capture());

        assertThat(captor.getValue().getTemplate()).isEqualTo("ORDER_CANCELLED_OUT_OF_STOCK");
    }

    @Test
    void sendInventoryAlert_shouldDispatchSendInventoryAlertCommand() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendInventoryAlert("ord-3", "cust-3", "alert@example.com",
                "prod-99", "trace-3");

        ArgumentCaptor<NotificationCommand> captor = ArgumentCaptor.forClass(NotificationCommand.class);
        verify(router).tell(captor.capture());

        assertThat(captor.getValue()).isInstanceOf(NotificationCommand.SendInventoryAlert.class);
        NotificationCommand.SendInventoryAlert cmd =
                (NotificationCommand.SendInventoryAlert) captor.getValue();
        assertThat(cmd.failedProductId()).isEqualTo("prod-99");
    }

    // ─── sendReturnConfirmation ───────────────────────────────────────────────

    @Test
    void sendReturnConfirmation_shouldPersistWithReturnApprovedTemplate() {
        when(repository.save(any(NotificationRecord.class))).thenAnswer(inv -> inv.getArgument(0));

        service.sendReturnConfirmation("ord-4", "cust-4", "return@example.com",
                "+9999999999", new BigDecimal("25.00"), "trace-4");

        ArgumentCaptor<NotificationRecord> captor = ArgumentCaptor.forClass(NotificationRecord.class);
        verify(repository).save(captor.capture());

        assertThat(captor.getValue().getTemplate()).isEqualTo("RETURN_APPROVED");
    }

    // ─── getById ─────────────────────────────────────────────────────────────

    @Test
    void getById_whenRecordExists_shouldReturnMappedResponse() {
        NotificationRecord record = new NotificationRecord(
                "ord-5", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");
        when(repository.findById("notif-id-1")).thenReturn(Optional.of(record));

        Optional<NotificationResponse> result = service.getById("notif-id-1");

        assertThat(result).isPresent();
        assertThat(result.get().orderId()).isEqualTo("ord-5");
    }

    @Test
    void getById_whenRecordNotFound_shouldReturnEmpty() {
        when(repository.findById("not-found")).thenReturn(Optional.empty());

        Optional<NotificationResponse> result = service.getById("not-found");

        assertThat(result).isEmpty();
    }

    // ─── getByOrderId ─────────────────────────────────────────────────────────

    @Test
    void getByOrderId_shouldReturnAllNotificationsForOrder() {
        NotificationRecord r1 = new NotificationRecord("ord-6", "EMAIL", "a@b.com", "ORDER_CONFIRMED", "{}");
        NotificationRecord r2 = new NotificationRecord("ord-6", "SMS",   "+111",    "ORDER_CONFIRMED", "{}");
        when(repository.findByOrderIdOrderByCreatedAtDesc("ord-6")).thenReturn(List.of(r1, r2));

        List<NotificationResponse> results = service.getByOrderId("ord-6");

        assertThat(results).hasSize(2);
        assertThat(results).allMatch(r -> r.orderId().equals("ord-6"));
    }

    @Test
    void getByOrderId_whenNoNotifications_shouldReturnEmptyList() {
        when(repository.findByOrderIdOrderByCreatedAtDesc("ord-none")).thenReturn(List.of());

        List<NotificationResponse> results = service.getByOrderId("ord-none");

        assertThat(results).isEmpty();
    }
}
