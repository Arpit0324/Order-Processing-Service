-- V2: idempotency constraint + missing indexes

-- Fix D: prevent double-reservation on Kafka event replay
ALTER TABLE inventory_reservations
  ADD CONSTRAINT uq_reservations_order_product UNIQUE (order_id, product_id);

-- Missing index: releaseReservation filters on (order_id, status)
CREATE INDEX idx_reservations_order_status ON inventory_reservations(order_id, status);

-- Missing index: optimistic lock UPDATE filters on (product_id, version)
CREATE INDEX idx_inventory_product_version ON inventory(product_id, version);

-- Configurable consumer group needs to be set in application.conf (kafka.consumer-group-id)
