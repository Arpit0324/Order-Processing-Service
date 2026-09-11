package com.ops.notification.provider;

// ── Delivery provider abstraction ────────────────────────────────────────────
// Decouples channel actors from a concrete vendor (SMTP / SES / SendGrid / Twilio).
// Implementations are chosen by configuration (enabled flag) via ProviderConfig.
public interface NotificationProvider {

    // Returns quickly; throws on failure so callers can report DeliveryAck.failed.
    void sendEmail(String to, String subject, String body);

    void sendSms(String to, String message);
}
