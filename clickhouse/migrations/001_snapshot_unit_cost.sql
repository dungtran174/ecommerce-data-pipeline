-- Existing historical cost remains NULL when the purchase-time value is unknown.
-- Kafka schema upgrades are handled by scripts/migrations/kafka_consumers.py, which
-- stops the source consumer before replacing a view and preserves group offsets.
ALTER TABLE bronze.orderdetails
    ADD COLUMN IF NOT EXISTS unit_cost_at_order Nullable(Decimal(15, 2)) AFTER subtotal_amount;
