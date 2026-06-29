package com.ops.notification.controller;

import com.ops.notification.dto.NotificationResponse;
import com.ops.notification.service.NotificationService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

// ── ENDPOINT LAYER — no business logic here ───────────────────────────────────
@RestController
@RequestMapping("/notifications")
public class NotificationController {

    private final NotificationService service;

    public NotificationController(NotificationService service) {
        this.service = service;
    }

    // GET /notifications/{id}
    @GetMapping("/{id}")
    public ResponseEntity<NotificationResponse> getById(
            @PathVariable String id,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId) {
        return service.getById(id)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    // GET /notifications?orderId=...
    @GetMapping
    public List<NotificationResponse> getByOrderId(
            @RequestParam String orderId,
            @RequestHeader(value = "X-Trace-Id", required = false) String traceId) {
        return service.getByOrderId(orderId);
    }
}
