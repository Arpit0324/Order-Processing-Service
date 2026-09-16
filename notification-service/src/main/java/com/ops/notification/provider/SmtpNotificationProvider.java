package com.ops.notification.provider;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import jakarta.mail.internet.MimeMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.MimeMessageHelper;

// ── Real email via SMTP (JavaMailSender); SMS not supported → throws ─────────
// Email calls are wrapped in a circuit breaker so a down SMTP server fails fast
// instead of piling work into actor mailboxes.
public class SmtpNotificationProvider implements NotificationProvider {

    private final JavaMailSender mailSender;
    private final String         fromAddress;
    private final CircuitBreaker emailBreaker;

    public SmtpNotificationProvider(JavaMailSender mailSender, String fromAddress,
                                    CircuitBreaker emailBreaker) {
        this.mailSender  = mailSender;
        this.fromAddress = fromAddress;
        this.emailBreaker = emailBreaker;
    }

    @Override
    public void sendEmail(String to, String subject, String body) {
        Runnable send = CircuitBreaker.decorateRunnable(emailBreaker, () -> {
            try {
                MimeMessage message = mailSender.createMimeMessage();
                MimeMessageHelper helper = new MimeMessageHelper(message, false, "UTF-8");
                helper.setFrom(fromAddress);
                helper.setTo(to);
                helper.setSubject(subject);
                helper.setText(body, false);
                mailSender.send(message);
            } catch (Exception ex) {
                throw new EmailDeliveryException("SMTP send failed: " + ex.getMessage(), ex);
            }
        });
        send.run();
    }

    @Override
    public void sendSms(String to, String message) {
        throw new UnsupportedOperationException(
                "SMS not supported by SmtpNotificationProvider — configure an SMS provider");
    }

    public static class EmailDeliveryException extends RuntimeException {
        public EmailDeliveryException(String message, Throwable cause) { super(message, cause); }
    }
}
