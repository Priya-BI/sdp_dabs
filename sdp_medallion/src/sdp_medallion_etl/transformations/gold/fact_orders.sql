-- =============================================================================
-- GOLD: fact_orders  (SCD Type 1)
-- Source : workspace.silver_sdp_dev.silver_orders (streaming)
-- Notes  : Larger table — upsert semantics (latest value wins per order_id).
--          Re-delivers of the same order_id overwrite the existing row.
--          _ingested_at and _source_file excluded from tracked columns.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.gold_sdp_dev.fact_orders
COMMENT "Order facts — SCD Type 1. Latest value per order_id is retained; no history kept."
TBLPROPERTIES ('quality' = 'gold')
CLUSTER BY (order_id);

APPLY CHANGES INTO workspace.gold_sdp_dev.fact_orders
FROM STREAM(workspace.silver_sdp_dev.silver_orders)
KEYS (order_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at, _source_file)
STORED AS SCD TYPE 1;
