package com.ops.notification;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.retry.annotation.EnableRetry;

// ── Boot sequence ─────────────────────────────────────────────────────────────
// 1. Spring Boot context starts
// 2. Flyway runs db/migration scripts (before JPA EntityManagerFactory)
// 3. JPA validates schema (ddl-auto=validate)
// 4. AkkaConfig beans created: ActorSystem + NotificationRouter spawned
// 5. KafkaListeners registered on: order.created, order.cancelled,
//    order.cancel.requested, order.returned
// 6. HTTP server starts on port 8083
@SpringBootApplication
@EnableKafka
@EnableRetry
public class NotificationApp {
    public static void main(String[] args) {
        SpringApplication.run(NotificationApp.class, args);
    }
}
