-- V2__create_akka_persistence_tables.sql
-- Order Service — Akka Persistence JDBC journal + snapshot store
-- Schema sourced from akka-persistence-jdbc 5.x standard DDL for PostgreSQL
-- ─────────────────────────────────────────────────────────────────────────────

-- Event journal: every OrderCreated, OrderConfirmed, etc. is appended here
CREATE TABLE order_events (
  ordering          BIGSERIAL    PRIMARY KEY,
  persistence_id    TEXT         NOT NULL,
  sequence_nr       BIGINT       NOT NULL,
  deleted           BOOLEAN      NOT NULL DEFAULT FALSE,
  writer            TEXT         NOT NULL,
  write_timestamp   BIGINT       NOT NULL,
  adapter_manifest  TEXT,
  event_ser_id      INT          NOT NULL,
  event_ser_manifest TEXT        NOT NULL,
  event_payload     BYTEA        NOT NULL,
  meta_ser_id       INT,
  meta_ser_manifest TEXT,
  meta_payload      BYTEA,
  CONSTRAINT order_events_pid_seq_unique UNIQUE (persistence_id, sequence_nr)
);

CREATE INDEX idx_order_events_pid ON order_events(persistence_id, sequence_nr);

-- Snapshot store: Akka takes a snapshot every 100 events to avoid long replay
CREATE TABLE order_snapshots (
  persistence_id        TEXT   NOT NULL,
  sequence_nr           BIGINT NOT NULL,
  created               BIGINT NOT NULL,
  snapshot_ser_id       INT    NOT NULL,
  snapshot_ser_manifest TEXT   NOT NULL,
  snapshot_payload      BYTEA  NOT NULL,
  meta_ser_id           INT,
  meta_ser_manifest     TEXT,
  meta_payload          BYTEA,
  PRIMARY KEY (persistence_id, sequence_nr)
);
