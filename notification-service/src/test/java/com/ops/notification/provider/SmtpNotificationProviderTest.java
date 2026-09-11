package com.ops.notification.provider;

import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig;
import jakarta.mail.internet.MimeMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.mail.MailSendException;
import org.springframework.mail.javamail.JavaMailSender;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

// ── SmtpNotificationProvider — delivery + circuit-breaker behaviour ───────────
class SmtpNotificationProviderTest {

    private JavaMailSender mailSender;

    @BeforeEach
    void setUp() {
        mailSender = mock(JavaMailSender.class);
        when(mailSender.createMimeMessage()).thenReturn(new MimeMessage((jakarta.mail.Session) null));
    }

    @Test
    void sendEmail_success_delegatesToMailSender() {
        var provider = new SmtpNotificationProvider(mailSender, "no-reply@ops.local",
                CircuitBreaker.ofDefaults("emailProvider"));

        provider.sendEmail("to@example.com", "Subject", "Body");

        verify(mailSender).send(any(MimeMessage.class));
    }

    @Test
    void sendEmail_failure_throwsEmailDeliveryException() {
        doThrow(new MailSendException("smtp down")).when(mailSender).send(any(MimeMessage.class));
        var provider = new SmtpNotificationProvider(mailSender, "no-reply@ops.local",
                CircuitBreaker.ofDefaults("emailProvider"));

        assertThatThrownBy(() -> provider.sendEmail("to@example.com", "Subject", "Body"))
                .isInstanceOf(SmtpNotificationProvider.EmailDeliveryException.class);
    }

    @Test
    void circuitBreaker_opensAfterRepeatedFailures_andFailsFast() {
        doThrow(new MailSendException("smtp down")).when(mailSender).send(any(MimeMessage.class));
        CircuitBreaker breaker = CircuitBreaker.of("emailProvider", CircuitBreakerConfig.custom()
                .slidingWindowSize(4)
                .minimumNumberOfCalls(4)
                .failureRateThreshold(50)
                .waitDurationInOpenState(Duration.ofSeconds(30))
                .build());
        var provider = new SmtpNotificationProvider(mailSender, "no-reply@ops.local", breaker);

        for (int i = 0; i < 4; i++) {
            assertThatThrownBy(() -> provider.sendEmail("to@example.com", "S", "B"))
                    .isInstanceOf(SmtpNotificationProvider.EmailDeliveryException.class);
        }

        assertThat(breaker.getState()).isEqualTo(CircuitBreaker.State.OPEN);
        // Once open, calls are rejected without invoking the mail sender.
        assertThatThrownBy(() -> provider.sendEmail("to@example.com", "S", "B"))
                .isInstanceOf(io.github.resilience4j.circuitbreaker.CallNotPermittedException.class);
    }

    @Test
    void sendSms_isUnsupported() {
        var provider = new SmtpNotificationProvider(mailSender, "no-reply@ops.local",
                CircuitBreaker.ofDefaults("emailProvider"));

        assertThatThrownBy(() -> provider.sendSms("+15551234567", "hi"))
                .isInstanceOf(UnsupportedOperationException.class);
    }
}
