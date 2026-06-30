package com.ops.notification.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.ops.notification.domain.NotificationRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class NotificationEventProducerTest {

    @Mock
    @SuppressWarnings("unchecked")
    private KafkaTemplate<String, String> kafkaTemplate;

    private ObjectMapper mapper;
    private NotificationEventProducer producer;

    @BeforeEach
    void setUp() {
        mapper   = new ObjectMapper().registerModule(new JavaTimeModule());
        producer = new NotificationEventProducer(kafkaTemplate, mapper);
    }

    @Test
    void publishNotificationSent_shouldPublishToNotifSentTopic() {
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(future);

        NotificationRecord record = new NotificationRecord("ord-1", "EMAIL", "u@e.com", "ORDER_CONFIRMED", "{}");
        producer.publishNotificationSent(record, "trace-1");

        verify(kafkaTemplate).send(eq("notif.sent"), eq("ord-1"), anyString());
    }

    @Test
    void publishNotificationSent_messageShouldContainEventTypeAndOrderId() throws Exception {
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), messageCaptor.capture())).thenReturn(future);

        NotificationRecord record = new NotificationRecord("ord-2", "EMAIL", "u@e.com", "ORDER_CANCELLED", "{}");
        producer.publishNotificationSent(record, "trace-2");

        String publishedJson = messageCaptor.getValue();
        assertThat(publishedJson).contains("NOTIFICATION_SENT");
        assertThat(publishedJson).contains("ord-2");
        assertThat(publishedJson).contains("trace-2");
    }

    @Test
    void publishNotificationSent_whenKafkaSendFails_shouldNotThrow() {
        CompletableFuture<SendResult<String, String>> failedFuture = new CompletableFuture<>();
        failedFuture.completeExceptionally(new RuntimeException("Kafka unavailable"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failedFuture);

        NotificationRecord record = new NotificationRecord("ord-3", "SMS", "+123", "ORDER_CONFIRMED", "{}");

        // Should not throw — Kafka failures are async and logged only
        producer.publishNotificationSent(record, "trace-3");
    }

    @Test
    void publishNotificationSent_messageShouldContainNotifIdAndChannel() throws Exception {
        ArgumentCaptor<String> messageCaptor = ArgumentCaptor.forClass(String.class);
        CompletableFuture<SendResult<String, String>> future = CompletableFuture.completedFuture(mock(SendResult.class));
        when(kafkaTemplate.send(anyString(), anyString(), messageCaptor.capture())).thenReturn(future);

        NotificationRecord record = new NotificationRecord("ord-4", "SMS", "+9999", "RETURN_APPROVED", "{}");
        producer.publishNotificationSent(record, "trace-4");

        String json = messageCaptor.getValue();
        assertThat(json).contains("SMS");
        assertThat(json).contains("RETURN_APPROVED");
        assertThat(json).contains(record.getId());
    }
}
