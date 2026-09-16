package com.ops.notification.provider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// ── Default provider — logs instead of sending (safe for local dev) ──────────
public class StubNotificationProvider implements NotificationProvider {

    private static final Logger log = LoggerFactory.getLogger(StubNotificationProvider.class);

    @Override
    public void sendEmail(String to, String subject, String body) {
        log.debug("[STUB] email to={} subject='{}' body='{}'", to, subject, body);
    }

    @Override
    public void sendSms(String to, String message) {
        log.debug("[STUB] sms to={} message='{}'", to, message);
    }
}
