package com.ops.notification.actor;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ── SmsNotificationActor — dispatches SMS via Twilio (stub) ──────────────────
public class SmsNotificationActor extends AbstractBehavior<SmsNotificationActor.SmsCommand> {

    private static final Logger log = LoggerFactory.getLogger(SmsNotificationActor.class);

    public record SmsCommand(
            String to,
            String message,
            String orderId,
            String traceId
    ) {}

    public static Behavior<SmsCommand> create() {
        return Behaviors.setup(SmsNotificationActor::new);
    }

    private SmsNotificationActor(ActorContext<SmsCommand> context) {
        super(context);
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
        } catch (Exception ex) {
            log.error("SMS delivery failed to={} orderId={}: {}", cmd.to(), cmd.orderId(), ex.getMessage());
            throw ex;
        }
        return this;
    }

    // ── Replace with Twilio SDK call ──────────────────────────────────────────
    private void sendSms(SmsCommand cmd) {
        // TODO: Twilio.init(ACCOUNT_SID, AUTH_TOKEN);
        // Message.creator(new PhoneNumber(cmd.to()), new PhoneNumber(FROM), cmd.message()).create();
        log.debug("[STUB] Would send SMS to={} message='{}'", cmd.to(), cmd.message());
    }
}
