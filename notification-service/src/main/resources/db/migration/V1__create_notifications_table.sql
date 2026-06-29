-- V1__create_notifications_table.sql
-- Notification Service — initial schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE notifications (
  id          UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id    UUID         NOT NULL,
  channel     VARCHAR(10)  NOT NULL
                           CONSTRAINT notifications_channel_check
                           CHECK (channel IN ('EMAIL','SMS','PUSH')),
  recipient   TEXT         NOT NULL,
  template    TEXT         NOT NULL,
  payload     JSONB,
  status      VARCHAR(10)  NOT NULL DEFAULT 'PENDING'
                           CONSTRAINT notifications_status_check
                           CHECK (status IN ('PENDING','DELIVERED','FAILED')),
  attempts    INT          NOT NULL DEFAULT 0,
  sent_at     TIMESTAMPTZ,
  error_msg   TEXT,
  created_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_notifications_order  ON notifications(order_id);
-- Partial index — only unresolved notifications (PENDING/FAILED are minority)
CREATE INDEX idx_notifications_pending ON notifications(created_at)
  WHERE status IN ('PENDING', 'FAILED');
