package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import com.ops.notification.provider.NotificationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ── SmsNotificationActor — dispatches SMS via the configured provider ────────
public class SmsNotificationActor extends AbstractBehavior<SmsNotificationActor.SmsCommand> {

    private static final Logger log = LoggerFactory.getLogger(SmsNotificationActor.class);

    private final NotificationProvider provider;

    public record SmsCommand(
            String to,
            String message,
            String orderId,
            String traceId,
            ActorRef<DeliveryAck> replyTo
    ) {}

    public static Behavior<SmsCommand> create(NotificationProvider provider) {
        return Behaviors.setup(ctx -> new SmsNotificationActor(ctx, provider));
    }

    private SmsNotificationActor(ActorContext<SmsCommand> context, NotificationProvider provider) {
        super(context);
        this.provider = provider;
    }

    @Override
    public Receive<SmsCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(SmsCommand.class, this::onSms)
                .build();
    }

    private Behavior<SmsCommand> onSms(SmsCommand cmd) {
        try {
            sendSms(cmd);
            log.info("SMS sent to={} orderId={} traceId={}", cmd.to(), cmd.orderId(), cmd.traceId());
            cmd.replyTo().tell(DeliveryAck.ok());
        } catch (Exception ex) {
            log.error("SMS delivery failed to={} orderId={}: {}", cmd.to(), cmd.orderId(), ex.getMessage());
            // Report failure via ack instead of throwing — retries are driven
            // by the service layer / Kafka, not by supervisor restart loops.
            cmd.replyTo().tell(DeliveryAck.failed(ex.getMessage()));
        }
        return this;
    }

    // ── Send via the configured provider (stub or real SMS gateway) ─────────
    private void sendSms(SmsCommand cmd) {
        provider.sendSms(cmd.to(), cmd.message());
    }
}
