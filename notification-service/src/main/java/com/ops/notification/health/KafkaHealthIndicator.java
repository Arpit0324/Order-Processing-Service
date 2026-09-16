package com.ops.notification.health;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

// ── Reports Kafka broker reachability in the readiness probe ─────────────────
// Notification work arrives over Kafka; if the broker is unreachable the pod
// should be pulled from readiness so it stops being counted as available.
// Uses a short timeout so a slow broker cannot stall the probe.
@Component("kafka")
public class KafkaHealthIndicator implements HealthIndicator {

    private static final int TIMEOUT_MS = 2000;

    private final KafkaAdmin kafkaAdmin;

    public KafkaHealthIndicator(KafkaAdmin kafkaAdmin) {
        this.kafkaAdmin = kafkaAdmin;
    }

    @Override
    public Health health() {
        try (AdminClient admin = AdminClient.create(kafkaAdmin.getConfigurationProperties())) {
            DescribeClusterResult cluster = admin.describeCluster();
            int nodeCount = cluster.nodes().get(TIMEOUT_MS, TimeUnit.MILLISECONDS).size();
            String clusterId = cluster.clusterId().get(TIMEOUT_MS, TimeUnit.MILLISECONDS);
            if (nodeCount == 0) {
                return Health.down().withDetail("kafka", "no brokers available").build();
            }
            return Health.up()
                    .withDetail("clusterId", clusterId)
                    .withDetail("nodes", nodeCount)
                    .build();
        } catch (Exception ex) {
            return Health.down().withDetail("kafka", ex.getMessage()).build();
        }
    }
}
