-- =============================================================================
-- GOLD: dim_customers  (SCD Type 2)
-- Source : workspace.silver_sdp_dev.silver_customers (streaming)
-- Notes  : Small table — full history tracking. Any change to name, email,
--          membership_level, region, or is_valid closes the old row
--          (__END_AT set) and opens a new one (__START_AT set).
--          Query WHERE __END_AT IS NULL to get current records.
--          _ingested_at and _source_file excluded to avoid false history rows.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.gold_sdp_dev.dim_customers
COMMENT "Customer dimension with full SCD Type 2 history. Use WHERE __END_AT IS NULL for current state."
TBLPROPERTIES ('quality' = 'gold')
CLUSTER BY (customer_id);

APPLY CHANGES INTO workspace.gold_sdp_dev.dim_customers
FROM STREAM(workspace.silver_sdp_dev.silver_customers)
KEYS (customer_id)
SEQUENCE BY _ingested_at
COLUMNS * EXCEPT (_ingested_at, _source_file)
STORED AS SCD TYPE 2;
