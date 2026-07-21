package com.ops.notification.actor;

import org.apache.pekko.actor.typed.ActorRef;
import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.SupervisorStrategy;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import java.time.Duration;

// ── NotificationRouter — receives commands and routes to channel actors ────────
// Email pool: 4 actors  (most notifications are email)
// SMS pool:   2 actors
// Supervised with restart-on-failure backoff per actor
public class NotificationRouter extends AbstractBehavior<NotificationCommand> {

    private final ActorRef<EmailNotificationActor.EmailCommand> emailActor;
    private final ActorRef<SmsNotificationActor.SmsCommand>     smsActor;

    public static Behavior<NotificationCommand> create() {
        return Behaviors.setup(NotificationRouter::new);
    }

    private NotificationRouter(ActorContext<NotificationCommand> context) {
        super(context);

        // Supervised child actors — restart with backoff on failure
        var emailBehavior = Behaviors.supervise(EmailNotificationActor.create())
                .onFailure(Exception.class,
                        SupervisorStrategy.restartWithBackoff(
                                Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2));

        var smsBehavior = Behaviors.supervise(SmsNotificationActor.create())
                .onFailure(Exception.class,
                        SupervisorStrategy.restartWithBackoff(
                                Duration.ofSeconds(1), Duration.ofSeconds(30), 0.2));

        emailActor = context.spawn(emailBehavior, "email-notifier");
        smsActor   = context.spawn(smsBehavior, "sms-notifier");
    }

    @Override
    public Receive<NotificationCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(NotificationCommand.SendOrderConfirmation.class, this::onOrderConfirmation)
                .onMessage(NotificationCommand.SendOrderCancellation.class, this::onOrderCancellation)
                .onMessage(NotificationCommand.SendInventoryAlert.class,    this::onInventoryAlert)
                .onMessage(NotificationCommand.SendReturnConfirmation.class, this::onReturnConfirmation)
                .onMessage(NotificationCommand.SendRefundInitiated.class,   this::onRefundInitiated)
                .build();
    }

    private Behavior<NotificationCommand> onOrderConfirmation(NotificationCommand.SendOrderConfirmation cmd) {
        emailActor.tell(new EmailNotificationActor.EmailCommand(
                cmd.email(), "ORDER_CONFIRMED",
                "Order " + cmd.orderId() + " confirmed. Total: $" + cmd.totalAmount(),
                cmd.orderId(), cmd.traceId()));

        if (cmd.phone() != null && !cmd.phone().isBlank()) {
            smsActor.tell(new SmsNotificationActor.SmsCommand(
                    cmd.phone(), "Order " + cmd.orderId() + " confirmed!", cmd.orderId(), cmd.traceId()));
        }
        return this;
    }

    private Behavior<NotificationCommand> onOrderCancellation(NotificationCommand.SendOrderCancellation cmd) {
        emailActor.tell(new EmailNotificationActor.EmailCommand(
                cmd.email(), "ORDER_CANCELLED",
                "Order " + cmd.orderId() + " has been cancelled. Reason: " + cmd.reason(),
                cmd.orderId(), cmd.traceId()));
        return this;
    }

    private Behavior<NotificationCommand> onInventoryAlert(NotificationCommand.SendInventoryAlert cmd) {
        emailActor.tell(new EmailNotificationActor.EmailCommand(
                cmd.email(), "ORDER_CANCELLED_OUT_OF_STOCK",
                "Sorry, order " + cmd.orderId() + " was cancelled due to insufficient stock for product " + cmd.failedProductId(),
                cmd.orderId(), cmd.traceId()));
        return this;
    }

    private Behavior<NotificationCommand> onReturnConfirmation(NotificationCommand.SendReturnConfirmation cmd) {
        emailActor.tell(new EmailNotificationActor.EmailCommand(
                cmd.email(), "RETURN_APPROVED",
                "Return approved for order " + cmd.orderId() + ". Refund of $" + cmd.refundAmount() + " initiated.",
                cmd.orderId(), cmd.traceId()));

        if (cmd.phone() != null && !cmd.phone().isBlank()) {
            smsActor.tell(new SmsNotificationActor.SmsCommand(
                    cmd.phone(), "Return approved. Refund of $" + cmd.refundAmount() + " initiated.",
                    cmd.orderId(), cmd.traceId()));
        }
        return this;
    }

    private Behavior<NotificationCommand> onRefundInitiated(NotificationCommand.SendRefundInitiated cmd) {
        emailActor.tell(new EmailNotificationActor.EmailCommand(
                cmd.email(), "REFUND_INITIATED",
                "Refund of $" + cmd.refundAmount() + " has been initiated for order " + cmd.orderId() + ". Allow 3-5 business days.",
                cmd.orderId(), cmd.traceId()));
        return this;
    }
}
