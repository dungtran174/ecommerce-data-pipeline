-- Idempotent upgrade for existing ClickHouse volumes. Kafka engine tables do
-- not support ADD COLUMN, so recreate this consumer with the same group name.
-- The committed group offset lives in Kafka and is retained across recreation.
DROP TABLE IF EXISTS bronze.mv_orderdetails;

DROP TABLE IF EXISTS bronze.kafka_orderdetails;
CREATE TABLE bronze.kafka_orderdetails (
    id Int32,
    order_id Int32,
    product_id Int32,
    quantity Int32,
    product_price Nullable(String),
    product_tax Nullable(String),
    subtotal_amount Nullable(String),
    unit_cost_at_order Nullable(String),
    created_at Int64,
    __op String,
    __table String,
    __source_ts_ms Int64
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:29092',
    kafka_topic_list = 'ecommerce_cdc.public.orderdetails',
    kafka_group_name = 'clickhouse_bronze_orderdetails',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 4,
    kafka_skip_broken_messages = 100;

ALTER TABLE bronze.orderdetails
    ADD COLUMN IF NOT EXISTS unit_cost_at_order Nullable(Decimal(15, 2)) AFTER subtotal_amount;

CREATE MATERIALIZED VIEW IF NOT EXISTS bronze.mv_orderdetails TO bronze.orderdetails AS
SELECT
    id, order_id, product_id, quantity,
    toDecimal64OrDefault(product_price, 2, toDecimal64(0, 2)) AS product_price,
    toDecimal64OrDefault(product_tax, 2, toDecimal64(0, 2)) AS product_tax,
    toDecimal64OrDefault(subtotal_amount, 2, toDecimal64(0, 2)) AS subtotal_amount,
    toDecimal64OrNull(unit_cost_at_order, 2) AS unit_cost_at_order,
    COALESCE(fromUnixTimestamp64Milli(created_at), now()) AS created_at,
    _offset AS _version,
    toUInt8(__op = 'd') AS _deleted
FROM bronze.kafka_orderdetails;
