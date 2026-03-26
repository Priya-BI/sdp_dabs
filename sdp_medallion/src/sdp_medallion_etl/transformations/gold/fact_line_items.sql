-- =============================================================================
-- GOLD: fact_line_items  (SCD Type 1)
-- Source : workspace.silver_sdp_dev.silver_line_items (streaming)
-- Notes  : Largest table (~1,500 rows) — upsert semantics (latest value wins
--          per line_item_id). Includes computed line_total from silver.
--          _ingested_at and _source_file excluded from tracked columns.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.gold_sdp_dev.fact_line_items
COMMENT "Line item facts — SCD Type 1. Latest value per line_item_id is retained; no history kept."
TBLPROPERTIES ('quality' = 'gold')
CLUSTER BY (line_item_id);

APPLY CHANGES INTO workspace.gold_sdp_dev.fact_line_items
FROM STREAM(workspace.silver_sdp_dev.silver_line_items)
KEYS (line_item_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at, _source_file)
STORED AS SCD TYPE 1;
