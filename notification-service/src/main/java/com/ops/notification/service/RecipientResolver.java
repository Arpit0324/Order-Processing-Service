package com.ops.notification.service;

// ── Recipient resolution seam ────────────────────────────────────────────────
// Order events may carry a recipient snapshot (customerEmail/customerPhone).
// Until order-service populates those fields, this falls back to a placeholder
// so the pipeline keeps working end-to-end in development.
public final class RecipientResolver {

    private RecipientResolver() {}

    public static String resolveEmail(String eventEmail, String orderId) {
        return isUsable(eventEmail) ? eventEmail : orderId + "@placeholder.ops";
    }

    public static String resolvePhone(String eventPhone) {
        return isUsable(eventPhone) ? eventPhone : null;
    }

    private static boolean isUsable(String value) {
        return value != null && !value.isBlank();
    }
}
