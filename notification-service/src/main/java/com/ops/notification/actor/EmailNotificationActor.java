package com.ops.notification.actor;

import org.apache.pekko.actor.typed.Behavior;
import org.apache.pekko.actor.typed.javadsl.AbstractBehavior;
import org.apache.pekko.actor.typed.javadsl.ActorContext;
import org.apache.pekko.actor.typed.javadsl.Behaviors;
import org.apache.pekko.actor.typed.javadsl.Receive;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ── EmailNotificationActor — dispatches emails via JavaMailSender ─────────────
// In a real system: inject JavaMailSender via constructor and send via SMTP.
// Here we log as placeholder — swap the sendEmail() body for real mail sending.
public class EmailNotificationActor extends AbstractBehavior<EmailNotificationActor.EmailCommand> {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationActor.class);

    public record EmailCommand(
            String to,
            String template,
            String body,
            String orderId,
            String traceId
    ) {}

    public static Behavior<EmailCommand> create() {
        return Behaviors.setup(EmailNotificationActor::new);
    }

    private EmailNotificationActor(ActorContext<EmailCommand> context) {
        super(context);
    }

    @Override
    public Receive<EmailCommand> createReceive() {
        return newReceiveBuilder()
                .onMessage(EmailCommand.class, this::onEmail)
                .build();
    }

    private Behavior<EmailCommand> onEmail(EmailCommand cmd) {
        try {
            sendEmail(cmd);
            log.info("Email sent template={} to={} orderId={} traceId={}",
                    cmd.template(), cmd.to(), cmd.orderId(), cmd.traceId());
        } catch (Exception ex) {
            log.error("Email delivery failed template={} to={} orderId={}: {}",
                    cmd.template(), cmd.to(), cmd.orderId(), ex.getMessage());
            throw ex; // let supervisor restart with backoff
        }
        return this;
    }

    // ── Replace with JavaMailSender.send(MimeMessageHelper) ──────────────────
    private void sendEmail(EmailCommand cmd) {
        // TODO: inject JavaMailSender, build MimeMessage, send
        // MimeMessage message = mailSender.createMimeMessage();
        // MimeMessageHelper helper = new MimeMessageHelper(message, true);
        // helper.setTo(cmd.to());
        // helper.setSubject(subjectFor(cmd.template()));
        // helper.setText(cmd.body(), true);
        // mailSender.send(message);
        log.debug("[STUB] Would send email to={} template={} body='{}'", cmd.to(), cmd.template(), cmd.body());
    }
}
