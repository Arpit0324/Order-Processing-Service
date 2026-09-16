package com.ops.notification.config;

import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry;
import com.ops.notification.provider.NotificationProvider;
import com.ops.notification.provider.SmtpNotificationProvider;
import com.ops.notification.provider.StubNotificationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.mail.MailProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.mail.javamail.JavaMailSenderImpl;

import java.util.Properties;

// ── Chooses the delivery provider based on configuration ─────────────────────
// Dev default: stub (no real mail/SMS). Set NOTIFICATION_EMAIL_ENABLED=true and
// provide SMTP_* env vars to switch to real delivery. A Twilio/other SMS
// provider can be added here behind NOTIFICATION_SMS_ENABLED without touching
// the actors.
@Configuration
public class ProviderConfig {

    private static final Logger log = LoggerFactory.getLogger(ProviderConfig.class);

    @Bean
    @ConditionalOnProperty(name = "notification.email.enabled", havingValue = "true")
    public JavaMailSender javaMailSender(
            @Value("${notification.email.host}") String host,
            @Value("${notification.email.port}") int port,
            @Value("${notification.email.username}") String username,
            @Value("${notification.email.password}") String password) {
        var sender = new JavaMailSenderImpl();
        sender.setHost(host);
        sender.setPort(port);
        if (username != null && !username.isBlank()) {
            sender.setUsername(username);
            sender.setPassword(password);
        }
        Properties props = sender.getJavaMailProperties();
        props.put("mail.transport.protocol", "smtp");
        props.put("mail.smtp.auth", String.valueOf(username != null && !username.isBlank()));
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.timeout", "5000");
        props.put("mail.smtp.connectiontimeout", "5000");
        log.info("SMTP email delivery ENABLED host={}:{}", host, port);
        return sender;
    }

    @Bean
    @ConditionalOnProperty(name = "notification.email.enabled", havingValue = "true")
    public NotificationProvider smtpProvider(JavaMailSender mailSender,
                                             @Value("${notification.email.from}") String from,
                                             CircuitBreakerRegistry registry) {
        return new SmtpNotificationProvider(mailSender, from, registry.circuitBreaker("emailProvider"));
    }

    @Bean
    @ConditionalOnProperty(name = "notification.email.enabled", havingValue = "false", matchIfMissing = true)
    public NotificationProvider stubProvider() {
        log.info("Notification delivery in STUB mode (no real email/SMS)");
        return new StubNotificationProvider();
    }
}
