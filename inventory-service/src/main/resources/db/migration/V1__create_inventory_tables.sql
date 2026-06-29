-- V1__create_inventory_tables.sql
-- Inventory Service — initial schema
-- ─────────────────────────────────────────────────────────────────────────────
CREATE EXTENSION IF NOT EXISTS "pgcrypto";

CREATE TABLE inventory (
  product_id   UUID         PRIMARY KEY,
  sku          TEXT         NOT NULL UNIQUE,
  name         TEXT         NOT NULL,
  quantity     INT          NOT NULL DEFAULT 0 CHECK (quantity >= 0),
  reserved_qty INT          NOT NULL DEFAULT 0 CHECK (reserved_qty >= 0),
  version      BIGINT       NOT NULL DEFAULT 0,  -- optimistic locking
  updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now(),
  -- Derived guard: available = quantity - reserved_qty >= 0
  CONSTRAINT inventory_available_check CHECK (quantity >= reserved_qty)
);

-- Tracks individual reservations per order (enables release on cancel)
CREATE TABLE inventory_reservations (
  id           UUID         PRIMARY KEY DEFAULT gen_random_uuid(),
  order_id     UUID         NOT NULL,
  product_id   UUID         NOT NULL REFERENCES inventory(product_id) ON DELETE RESTRICT,
  quantity     INT          NOT NULL CHECK (quantity > 0),
  status       VARCHAR(10)  NOT NULL DEFAULT 'ACTIVE'
                            CONSTRAINT reservations_status_check
                            CHECK (status IN ('ACTIVE','RELEASED','COMMITTED')),
  created_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

CREATE INDEX idx_reservations_order   ON inventory_reservations(order_id);
-- Only index active reservations (partial index = smaller, faster)
CREATE INDEX idx_reservations_product_active ON inventory_reservations(product_id)
  WHERE status = 'ACTIVE';
