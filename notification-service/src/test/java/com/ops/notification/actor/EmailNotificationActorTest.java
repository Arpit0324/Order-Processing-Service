package com.ops.notification.actor;

import com.ops.notification.provider.NotificationProvider;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.pekko.actor.testkit.typed.javadsl.ActorTestKit;
import org.apache.pekko.actor.testkit.typed.javadsl.TestProbe;
import org.apache.pekko.actor.typed.ActorRef;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

// ── Pekko TestKit — EmailNotificationActor delivery acknowledgement protocol ──
class EmailNotificationActorTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    void onProviderSuccess_repliesDeliveryAckOk_andRecordsLatency() {
        NotificationProvider provider = mock(NotificationProvider.class);
        SimpleMeterRegistry registry  = new SimpleMeterRegistry();

        ActorRef<EmailNotificationActor.EmailCommand> actor =
                testKit.spawn(EmailNotificationActor.create(provider, registry));
        TestProbe<DeliveryAck> probe = testKit.createTestProbe(DeliveryAck.class);

        actor.tell(new EmailNotificationActor.EmailCommand(
                "to@example.com", "ORDER_CONFIRMED", "body", "ord-1", "trace-1", probe.getRef()));

        DeliveryAck reply = probe.receiveMessage();
        assertThat(reply.success()).isTrue();
        verify(provider).sendEmail(eq("to@example.com"), anyString(), eq("body"));
        assertThat(registry.timer("notification.email.latency").count()).isEqualTo(1L);
    }

    @Test
    void onProviderFailure_repliesDeliveryAckFailed_withError() {
        NotificationProvider provider = mock(NotificationProvider.class);
        doThrow(new RuntimeException("smtp down"))
                .when(provider).sendEmail(anyString(), anyString(), anyString());

        ActorRef<EmailNotificationActor.EmailCommand> actor =
                testKit.spawn(EmailNotificationActor.create(provider, new SimpleMeterRegistry()));
        TestProbe<DeliveryAck> probe = testKit.createTestProbe(DeliveryAck.class);

        actor.tell(new EmailNotificationActor.EmailCommand(
                "to@example.com", "ORDER_CONFIRMED", "body", "ord-1", "trace-1", probe.getRef()));

        DeliveryAck reply = probe.receiveMessage();
        assertThat(reply.success()).isFalse();
        assertThat(reply.error()).contains("smtp down");
    }
}
