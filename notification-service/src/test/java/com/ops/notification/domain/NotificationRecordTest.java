package com.ops.notification.domain;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class NotificationRecordTest {

    @Test
    void constructor_shouldSetDefaultStatusToPending() {
        NotificationRecord record = new NotificationRecord(
                "ord-1", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{\"amount\":\"99\"}");

        assertThat(record.getStatus()).isEqualTo("PENDING");
    }

    @Test
    void constructor_shouldAssignNonNullId() {
        NotificationRecord record = new NotificationRecord(
                "ord-1", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");

        assertThat(record.getId()).isNotNull().isNotBlank();
    }

    @Test
    void constructor_shouldSetAttemptsToZero() {
        NotificationRecord record = new NotificationRecord(
                "ord-1", "SMS", "+1234567890", "ORDER_CONFIRMED", "{}");

        assertThat(record.getAttempts()).isZero();
    }

    @Test
    void constructor_shouldSetCreatedAt() {
        NotificationRecord record = new NotificationRecord(
                "ord-1", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");

        assertThat(record.getCreatedAt()).isNotNull();
    }

    @Test
    void markDelivered_shouldUpdateStatusAndIncrementAttempts() {
        NotificationRecord record = new NotificationRecord(
                "ord-2", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");

        record.markDelivered();

        assertThat(record.getStatus()).isEqualTo("DELIVERED");
        assertThat(record.getAttempts()).isEqualTo(1);
        assertThat(record.getSentAt()).isNotNull();
    }

    @Test
    void markDelivered_multipleCallsShouldIncrementAttemptsEachTime() {
        NotificationRecord record = new NotificationRecord(
                "ord-3", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");

        record.markDelivered();
        record.markDelivered();

        assertThat(record.getAttempts()).isEqualTo(2);
    }

    @Test
    void markFailed_shouldUpdateStatusAndErrorMsg() {
        NotificationRecord record = new NotificationRecord(
                "ord-4", "EMAIL", "user@example.com", "ORDER_CONFIRMED", "{}");

        record.markFailed("SMTP connection failed");

        assertThat(record.getStatus()).isEqualTo("FAILED");
        assertThat(record.getErrorMsg()).isEqualTo("SMTP connection failed");
        assertThat(record.getAttempts()).isEqualTo(1);
    }

    @Test
    void markFailed_afterDelivered_shouldOverrideStatus() {
        NotificationRecord record = new NotificationRecord(
                "ord-5", "EMAIL", "user@example.com", "RETURN_APPROVED", "{}");
        record.markDelivered();
        record.markFailed("Retry failed");

        assertThat(record.getStatus()).isEqualTo("FAILED");
        assertThat(record.getAttempts()).isEqualTo(2);
    }

    @Test
    void constructor_shouldStoreAllFieldsCorrectly() {
        NotificationRecord record = new NotificationRecord(
                "ord-6", "SMS", "+447700000000", "ORDER_CANCELLED", "{\"reason\":\"x\"}");

        assertThat(record.getOrderId()).isEqualTo("ord-6");
        assertThat(record.getChannel()).isEqualTo("SMS");
        assertThat(record.getRecipient()).isEqualTo("+447700000000");
        assertThat(record.getTemplate()).isEqualTo("ORDER_CANCELLED");
        assertThat(record.getPayload()).isEqualTo("{\"reason\":\"x\"}");
    }

    @Test
    void twoRecords_shouldHaveDifferentIds() {
        NotificationRecord r1 = new NotificationRecord("ord-7", "EMAIL", "a@b.com", "ORDER_CONFIRMED", "{}");
        NotificationRecord r2 = new NotificationRecord("ord-7", "EMAIL", "a@b.com", "ORDER_CONFIRMED", "{}");

        assertThat(r1.getId()).isNotEqualTo(r2.getId());
    }
}
