package com.ops.notification.actor;

import com.ops.notification.provider.NotificationProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;

// ── Pekko TestKit — NotificationRouter routing + delivery metrics ─────────────
// Email is the persisted channel: its ack drives the DeliveryResult and the
// notification.delivery.{success,failure} counters.
class NotificationRouterTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    void deliveredEmail_returnsSuccessResult_andIncrementsSuccessCounter() {
        NotificationProvider provider = mock(NotificationProvider.class);
        SimpleMeterRegistry registry  = new SimpleMeterRegistry();

        ActorRef<NotificationCommand> router =
                testKit.spawn(NotificationRouter.create(provider, registry, 1, 1));
        TestProbe<DeliveryResult> probe = testKit.createTestProbe(DeliveryResult.class);

        router.tell(new NotificationCommand.SendOrderConfirmation(
                "notif-1", "ord-1", "cust-1", "to@example.com", "+15551234567",
                new BigDecimal("39.98"), "trace-1", probe.getRef()));

        DeliveryResult result = probe.receiveMessage();
        assertThat(result.notificationId()).isEqualTo("notif-1");
        assertThat(result.success()).isTrue();
        assertThat(registry.counter("notification.delivery.success").count()).isEqualTo(1.0);
    }

    @Test
    void failedEmail_returnsFailureResult_andIncrementsFailureCounter() {
        NotificationProvider provider = mock(NotificationProvider.class);
        doThrow(new RuntimeException("smtp down"))
                .when(provider).sendEmail(anyString(), anyString(), anyString());
        SimpleMeterRegistry registry = new SimpleMeterRegistry();

        ActorRef<NotificationCommand> router =
                testKit.spawn(NotificationRouter.create(provider, registry, 1, 1));
        TestProbe<DeliveryResult> probe = testKit.createTestProbe(DeliveryResult.class);

        router.tell(new NotificationCommand.SendOrderCancellation(
                "notif-2", "ord-2", "cust-2", "to@example.com", "CUSTOMER_REQUEST",
                "trace-2", probe.getRef()));

        DeliveryResult result = probe.receiveMessage();
        assertThat(result.notificationId()).isEqualTo("notif-2");
        assertThat(result.success()).isFalse();
        assertThat(result.error()).contains("smtp down");
        assertThat(registry.counter("notification.delivery.failure").count()).isEqualTo(1.0);
    }
}
