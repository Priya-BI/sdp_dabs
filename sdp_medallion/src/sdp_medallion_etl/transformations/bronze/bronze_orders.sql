-- =============================================================================
-- BRONZE: orders
-- Source : /Volumes/workspace/raw_data/raw_sdp/orders/*.json
-- Schema : order_id, customer_id, order_date, total_amount, status
-- Notes  : Schema evolution via rescue mode. CDF enabled for silver streaming.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE bronze_orders
COMMENT "Raw order records ingested from JSON source files. Schema evolution enabled."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name',
  'quality'                    = 'bronze'
)
CLUSTER BY (order_id)
AS
SELECT
  *,
  current_timestamp()              AS _ingested_at,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_modified_at
FROM STREAM read_files(
  '/Volumes/workspace/raw_data/raw_sdp/orders',
  format              => 'json',
  schemaHints         => 'order_id STRING, customer_id STRING, order_date DATE, total_amount DOUBLE, status STRING',
  schemaEvolutionMode => 'rescue'
);
