-- V2__idempotency_and_dead_letters.sql
-- Notification Service — idempotency key + dead-letter persistence
-- ─────────────────────────────────────────────────────────────────────────────

-- Idempotency key on notifications (eventId from the source Kafka event)
ALTER TABLE notifications ADD COLUMN IF NOT EXISTS event_id TEXT;

-- Unique per (event, template, channel) — duplicate Kafka deliveries become no-ops.
-- Partial index so legacy rows (event_id IS NULL) are unaffected.
CREATE UNIQUE INDEX IF NOT EXISTS ux_notifications_event
  ON notifications(event_id, template, channel)
  WHERE event_id IS NOT NULL;

-- Durable store for messages that exhausted @RetryableTopic retries
CREATE TABLE IF NOT EXISTS dead_letter_notifications (
  id                 UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  original_topic     TEXT         NOT NULL,
  original_partition INT,
  original_offset    BIGINT,
  message_key        TEXT,
  payload            TEXT,
  exception_type     TEXT,
  exception_message  TEXT,
  trace_id           TEXT,
  created_at         TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlt_created ON dead_letter_notifications(created_at);
