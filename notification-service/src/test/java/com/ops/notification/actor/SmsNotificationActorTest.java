package com.ops.notification.actor;

import com.ops.notification.provider.NotificationProvider;
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

// ── Pekko TestKit — SmsNotificationActor delivery acknowledgement protocol ────
class SmsNotificationActorTest {

    private static final ActorTestKit testKit = ActorTestKit.create();

    @AfterAll
    static void teardown() {
        testKit.shutdownTestKit();
    }

    @Test
    void onProviderSuccess_repliesDeliveryAckOk() {
        NotificationProvider provider = mock(NotificationProvider.class);

        ActorRef<SmsNotificationActor.SmsCommand> actor =
                testKit.spawn(SmsNotificationActor.create(provider));
        TestProbe<DeliveryAck> probe = testKit.createTestProbe(DeliveryAck.class);

        actor.tell(new SmsNotificationActor.SmsCommand(
                "+15551234567", "Order confirmed", "ord-1", "trace-1", probe.getRef()));

        DeliveryAck reply = probe.receiveMessage();
        assertThat(reply.success()).isTrue();
        verify(provider).sendSms(eq("+15551234567"), anyString());
    }

    @Test
    void onProviderFailure_repliesDeliveryAckFailed() {
        NotificationProvider provider = mock(NotificationProvider.class);
        doThrow(new RuntimeException("gateway timeout"))
                .when(provider).sendSms(anyString(), anyString());

        ActorRef<SmsNotificationActor.SmsCommand> actor =
                testKit.spawn(SmsNotificationActor.create(provider));
        TestProbe<DeliveryAck> probe = testKit.createTestProbe(DeliveryAck.class);

        actor.tell(new SmsNotificationActor.SmsCommand(
                "+15551234567", "Order confirmed", "ord-1", "trace-1", probe.getRef()));

        DeliveryAck reply = probe.receiveMessage();
        assertThat(reply.success()).isFalse();
        assertThat(reply.error()).contains("gateway timeout");
    }
}
