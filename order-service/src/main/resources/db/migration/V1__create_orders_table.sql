-- V1__create_orders_table.sql
-- Order Service — initial schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE orders (
  id               UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id      UUID         NOT NULL,
  status           VARCHAR(20)  NOT NULL DEFAULT 'PENDING'
                                CONSTRAINT orders_status_check
                                CHECK (status IN (
                                  'PENDING','CONFIRMED','CANCELLED',
                                  'FULFILLED','RETURN_REQUESTED','RETURNED'
                                )),
  items            JSONB        NOT NULL,
  total_amount     NUMERIC(12,2) NOT NULL CHECK (total_amount >= 0),
  shipping_address JSONB        NOT NULL,
  idempotency_key  TEXT         UNIQUE,
  cancelled_reason    TEXT,
  cancelled_at        TIMESTAMPTZ,
  return_requested_at TIMESTAMPTZ,
  return_reason       TEXT,
  returned_at         TIMESTAMPTZ,
  refund_amount       NUMERIC(12,2),
  created_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Customer order history (most common query)
CREATE INDEX idx_orders_customer_id  ON orders(customer_id);
-- Status filter (ops dashboards, saga queries)
CREATE INDEX idx_orders_status       ON orders(status);
-- Time-range queries
CREATE INDEX idx_orders_created_at   ON orders(created_at DESC);
-- Background job: find stale PENDING orders efficiently
CREATE INDEX idx_orders_stale_pending ON orders(created_at)
  WHERE status = 'PENDING';
-- Ops: find orders needing return review
CREATE INDEX idx_orders_return_pending ON orders(return_requested_at)
  WHERE status = 'RETURN_REQUESTED';
