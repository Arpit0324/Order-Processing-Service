package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.PreRestart;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import com.ops.notification.provider.NotificationProvider;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ── EmailNotificationActor — dispatches emails via the configured provider ──
// Provider is resolved by configuration (stub in dev, SMTP when enabled).
public class EmailNotificationActor extends AbstractBehavior<EmailNotificationActor.EmailCommand> {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationActor.class);

    private final NotificationProvider provider;
    private final Timer                sendTimer;
    private final MeterRegistry        registry;

    public record EmailCommand(
            String to,
            String template,
            String body,
            String orderId,
            String traceId,
            ActorRef<DeliveryAck> replyTo
    ) {}

    public static Behavior<EmailCommand> create(NotificationProvider provider, MeterRegistry registry) {
        return Behaviors.setup(ctx -> new EmailNotificationActor(ctx, provider, registry));
    }

    private EmailNotificationActor(ActorContext<EmailCommand> context, NotificationProvider provider,
                                   MeterRegistry registry) {
        super(context);
        this.provider  = provider;
        this.registry  = registry;
        this.sendTimer = registry.timer("notification.email.latency");
    }

    @Override
    public Receive<EmailCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(EmailCommand.class, this::onEmail)
                .onSignal(PreRestart.class, this::onPreRestart)
                .build();
    }

    private Behavior<EmailCommand> onEmail(EmailCommand cmd) {
        Timer.Sample sample = Timer.start(registry);
        try {
            sendEmail(cmd);
            log.info("Email sent template={} to={} orderId={} traceId={}",
                    cmd.template(), cmd.to(), cmd.orderId(), cmd.traceId());
            cmd.replyTo().tell(DeliveryAck.ok());
        } catch (Exception ex) {
            log.error("Email delivery failed template={} to={} orderId={}: {}",
                    cmd.template(), cmd.to(), cmd.orderId(), ex.getMessage());
            // Report failure via ack instead of throwing — retries are driven
            // by the service layer / Kafka, not by supervisor restart loops.
            cmd.replyTo().tell(DeliveryAck.failed(ex.getMessage()));
        } finally {
            sample.stop(sendTimer);
        }
        return this;
    }

    // ── Supervisor is about to restart this routee after a failure ───────────
    private Behavior<EmailCommand> onPreRestart(PreRestart signal) {
        registry.counter("notification.actor.restart", "actor", "email").increment();
        log.warn("EmailNotificationActor restarting");
        return this;
    }

    // ── Send via the configured provider (stub or SMTP) ─────────────────────
    private void sendEmail(EmailCommand cmd) {
        provider.sendEmail(cmd.to(), subjectFor(cmd.template()), cmd.body());
    }

    private static String subjectFor(String template) {
        return switch (template) {
            case "ORDER_CONFIRMED"             -> "Your order is confirmed";
            case "ORDER_CANCELLED"            -> "Your order was cancelled";
            case "ORDER_CANCELLED_OUT_OF_STOCK" -> "Order cancelled — out of stock";
            case "RETURN_APPROVED"            -> "Your return was approved";
            case "REFUND_INITIATED"           -> "Your refund is on its way";
            default                            -> "Notification from Ops";
        };
    }
}
