package com.ops.notification.controller;

import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.service.NotificationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class NotificationControllerTest {

    @Mock
    private NotificationService service;

    @InjectMocks
    private NotificationController controller;

    private NotificationResponse sampleResponse;

    @BeforeEach
    void setUp() {
        sampleResponse = new NotificationResponse(
                "notif-1", "ord-1", "EMAIL", "test@example.com",
                "ORDER_CONFIRMED", "PENDING", 0, null, null, Instant.now());
    }

    // ─── GET /notifications/{id} ──────────────────────────────────────────────

    @Test
    void getById_whenFound_shouldReturn200WithBody() {
        when(service.getById("notif-1")).thenReturn(Optional.of(sampleResponse));

        ResponseEntity<NotificationResponse> response = controller.getById("notif-1", "trace-1");

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(response.getBody()).isEqualTo(sampleResponse);
    }

    @Test
    void getById_whenNotFound_shouldReturn404() {
        when(service.getById("notif-missing")).thenReturn(Optional.empty());

        ResponseEntity<NotificationResponse> response = controller.getById("notif-missing", null);

        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.NOT_FOUND);
        assertThat(response.getBody()).isNull();
    }

    @Test
    void getById_shouldDelegateToService() {
        when(service.getById("notif-1")).thenReturn(Optional.of(sampleResponse));

        controller.getById("notif-1", "trace-abc");

        verify(service).getById("notif-1");
    }

    // ─── GET /notifications?orderId=... ──────────────────────────────────────

    @Test
    void getByOrderId_shouldReturnListFromService() {
        NotificationResponse r2 = new NotificationResponse(
                "notif-2", "ord-1", "SMS", "+123", "ORDER_CONFIRMED",
                "DELIVERED", 1, Instant.now(), null, Instant.now());
        when(service.getByOrderId("ord-1")).thenReturn(List.of(sampleResponse, r2));

        List<NotificationResponse> results = controller.getByOrderId("ord-1", "trace-2");

        assertThat(results).hasSize(2);
    }

    @Test
    void getByOrderId_whenNoResults_shouldReturnEmptyList() {
        when(service.getByOrderId("ord-empty")).thenReturn(List.of());

        List<NotificationResponse> results = controller.getByOrderId("ord-empty", null);

        assertThat(results).isEmpty();
    }

    @Test
    void getByOrderId_shouldDelegateToService() {
        when(service.getByOrderId("ord-1")).thenReturn(List.of(sampleResponse));

        controller.getByOrderId("ord-1", null);

        verify(service).getByOrderId("ord-1");
    }
}
