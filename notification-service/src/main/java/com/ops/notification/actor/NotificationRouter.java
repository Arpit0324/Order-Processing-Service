package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.apache.pekko.actor.typed.javadsl.Routers;
import com.ops.notification.provider.NotificationProvider;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import java.time.Duration;

// ── NotificationRouter — receives commands and routes to channel actors ────────
// Pool sizes are configurable via notification.actor.{email,sms}-pool-size and
// backed by supervised Pekko pool routers, so channel work is spread across a
// bounded set of routees (bulkhead) instead of a single serial actor.
// Email (the persisted channel) is acked; SMS is ancillary fire-and-forget.
public class NotificationRouter extends AbstractBehavior<NotificationCommand> {

    private static final Duration ASK_TIMEOUT = Duration.ofSeconds(10);

    private final ActorRef<EmailNotificationActor.EmailCommand> emailActor;
    private final ActorRef<SmsNotificationActor.SmsCommand>     smsActor;
    private final Counter delivered;
    private final Counter failed;

    public static Behavior<NotificationCommand> create(NotificationProvider provider, MeterRegistry registry,
                                                       int emailPoolSize, int smsPoolSize) {
        return Behaviors.setup(ctx -> new NotificationRouter(ctx, provider, registry, emailPoolSize, smsPoolSize));
    }

    private NotificationRouter(ActorContext<NotificationCommand> context,
                               NotificationProvider provider, MeterRegistry registry,
                               int emailPoolSize, int smsPoolSize) {
        super(context);
        this.delivered = registry.counter("notification.delivery.success");
        this.failed    = registry.counter("notification.delivery.failure");

        // Supervised routees — restart with backoff on failure — behind bounded
        // pool routers whose size is externally configurable.
        var emailBehavior = Behaviors.supervise(EmailNotificationActor.create(provider, registry))
                .onFailure(Exception.class,
                        SupervisorStrategy.restartWithBackoff(
                                Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2));

        var smsBehavior = Behaviors.supervise(SmsNotificationActor.create(provider))
                .onFailure(Exception.class,
                        SupervisorStrategy.restartWithBackoff(
                                Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2));

        emailActor = context.spawn(Routers.pool(Math.max(1, emailPoolSize), emailBehavior), "email-pool");
        smsActor   = context.spawn(Routers.pool(Math.max(1, smsPoolSize), smsBehavior), "sms-pool");
    }

    @Override
    public Receive<NotificationCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(NotificationCommand.SendOrderConfirmation.class, this::onOrderConfirmation)
                .onMessage(NotificationCommand.SendOrderCancellation.class, this::onOrderCancellation)
                .onMessage(NotificationCommand.SendInventoryAlert.class,    this::onInventoryAlert)
                .onMessage(NotificationCommand.SendReturnConfirmation.class, this::onReturnConfirmation)
                .onMessage(NotificationCommand.SendRefundInitiated.class,   this::onRefundInitiated)
                .onMessage(NotificationCommand.DeliveryAttempted.class,     this::onDeliveryAttempted)
                .build();
    }

    private Behavior<NotificationCommand> onOrderConfirmation(NotificationCommand.SendOrderConfirmation cmd) {
        askEmail(cmd, cmd.email(), "ORDER_CONFIRMED",
                "Order " + cmd.orderId() + " confirmed. Total: $" + cmd.totalAmount(),
                cmd.orderId(), cmd.traceId());

        tellSms(cmd.phone(), "Order " + cmd.orderId() + " confirmed!", cmd.orderId(), cmd.traceId());
        return this;
    }

    private Behavior<NotificationCommand> onOrderCancellation(NotificationCommand.SendOrderCancellation cmd) {
        askEmail(cmd, cmd.email(), "ORDER_CANCELLED",
                "Order " + cmd.orderId() + " has been cancelled. Reason: " + cmd.reason(),
                cmd.orderId(), cmd.traceId());
        return this;
    }

    private Behavior<NotificationCommand> onInventoryAlert(NotificationCommand.SendInventoryAlert cmd) {
        askEmail(cmd, cmd.email(), "ORDER_CANCELLED_OUT_OF_STOCK",
                "Sorry, order " + cmd.orderId() + " was cancelled due to insufficient stock for product " + cmd.failedProductId(),
                cmd.orderId(), cmd.traceId());
        return this;
    }

    private Behavior<NotificationCommand> onReturnConfirmation(NotificationCommand.SendReturnConfirmation cmd) {
        askEmail(cmd, cmd.email(), "RETURN_APPROVED",
                "Return approved for order " + cmd.orderId() + ". Refund of $" + cmd.refundAmount() + " initiated.",
                cmd.orderId(), cmd.traceId());

        tellSms(cmd.phone(), "Return approved. Refund of $" + cmd.refundAmount() + " initiated.",
                cmd.orderId(), cmd.traceId());
        return this;
    }

    private Behavior<NotificationCommand> onRefundInitiated(NotificationCommand.SendRefundInitiated cmd) {
        askEmail(cmd, cmd.email(), "REFUND_INITIATED",
                "Refund of $" + cmd.refundAmount() + " has been initiated for order " + cmd.orderId() + ". Allow 3-5 business days.",
                cmd.orderId(), cmd.traceId());
        return this;
    }

    // ── Ask the email channel actor and adapt its ack into our protocol ────────
    // Email is the persisted channel — its result drives the record status.
    private void askEmail(NotificationCommand cmd, String to, String template,
                          String body, String orderId, String traceId) {
        String notificationId = notificationIdOf(cmd);
        ActorRef<DeliveryResult> replyTo = replyToOf(cmd);
        getContext().ask(DeliveryAck.class, emailActor, ASK_TIMEOUT,
                ref -> new EmailNotificationActor.EmailCommand(to, template, body, orderId, traceId, ref),
                (ack, failure) -> new NotificationCommand.DeliveryAttempted(notificationId, replyTo, ack, failure));
    }

    // ── SMS is ancillary (no persisted record) — fire-and-forget, acks ignored ─
    private void tellSms(String phone, String message, String orderId, String traceId) {
        if (phone != null && !phone.isBlank()) {
            smsActor.tell(new SmsNotificationActor.SmsCommand(phone, message, orderId, traceId,
                    getContext().getSystem().ignoreRef()));
        }
    }

    // ── Channel ack arrived — forward the terminal result to the service layer ─
    private Behavior<NotificationCommand> onDeliveryAttempted(NotificationCommand.DeliveryAttempted msg) {
        if (msg.ack() != null) {
            if (msg.ack().success()) {
                delivered.increment();
                msg.replyTo().tell(DeliveryResult.ok(msg.notificationId()));
            } else {
                failed.increment();
                msg.replyTo().tell(DeliveryResult.failed(msg.notificationId(), msg.ack().error()));
            }
        } else {
            failed.increment();
            String error = msg.failure() != null
                    ? String.valueOf(msg.failure().getMessage())
                    : "no response from channel actor";
            msg.replyTo().tell(DeliveryResult.failed(msg.notificationId(), error));
        }
        return this;
    }

    private static String notificationIdOf(NotificationCommand cmd) {
        return switch (cmd) {
            case NotificationCommand.SendOrderConfirmation c -> c.notificationId();
            case NotificationCommand.SendOrderCancellation c -> c.notificationId();
            case NotificationCommand.SendInventoryAlert    c -> c.notificationId();
            case NotificationCommand.SendReturnConfirmation c -> c.notificationId();
            case NotificationCommand.SendRefundInitiated   c -> c.notificationId();
            case NotificationCommand.DeliveryAttempted     c -> c.notificationId();
        };
    }

    private static ActorRef<DeliveryResult> replyToOf(NotificationCommand cmd) {
        return switch (cmd) {
            case NotificationCommand.SendOrderConfirmation c -> c.replyTo();
            case NotificationCommand.SendOrderCancellation c -> c.replyTo();
            case NotificationCommand.SendInventoryAlert    c -> c.replyTo();
            case NotificationCommand.SendReturnConfirmation c -> c.replyTo();
            case NotificationCommand.SendRefundInitiated   c -> c.replyTo();
            case NotificationCommand.DeliveryAttempted     c -> c.replyTo();
        };
    }
}
