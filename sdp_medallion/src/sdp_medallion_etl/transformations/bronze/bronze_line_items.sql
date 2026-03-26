-- =============================================================================
-- BRONZE: line_items
-- Source : /Volumes/workspace/raw_data/raw_sdp/line_items/*.json
-- Schema : line_item_id, order_id, product_name, quantity, unit_price
-- Notes  : Largest table (~1,500 rows). Schema evolution + rescue mode.
--          CDF enabled for silver streaming.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE bronze_line_items
COMMENT "Raw line item records ingested from JSON source files. Schema evolution enabled."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name',
  'quality'                    = 'bronze'
)
CLUSTER BY (line_item_id)
AS
SELECT
  *,
  current_timestamp()              AS _ingested_at,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_modified_at
FROM STREAM read_files(
  '/Volumes/workspace/raw_data/raw_sdp/line_items',
  format              => 'json',
  schemaHints         => 'line_item_id STRING, order_id STRING, product_name STRING, quantity INT, unit_price DOUBLE',
  schemaEvolutionMode => 'rescue'
);
