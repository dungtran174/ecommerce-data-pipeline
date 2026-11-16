-- Manual inspection of the published fact. The scheduled DAG now performs
-- real materialization via helpers.warehouse_refresh (not this SELECT).
SELECT count() AS product_rows, sum(gmv) AS gmv
FROM gold.FACT_SALES_PRODUCT
WHERE snapshot_id = (SELECT snapshot_id FROM warehouse.active_snapshot LIMIT 1);
