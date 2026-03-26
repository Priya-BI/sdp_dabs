-- =============================================================================
-- BRONZE: customers
-- Source : /Volumes/workspace/raw_data/raw_sdp/customers/*.json
-- Schema : customer_id, name, email, membership_level, region
-- Notes  : Schema evolution via rescue mode (_rescued_data captures new fields).
--          CDF enabled so silver can stream changes. Cluster by PK.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE bronze_customers
COMMENT "Raw customer records ingested from JSON source files. Schema evolution enabled; unexpected fields land in _rescued_data."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.columnMapping.mode'   = 'name',
  'quality'                    = 'bronze'
)
CLUSTER BY (customer_id)
AS
SELECT
  *,
  current_timestamp()              AS _ingested_at,
  _metadata.file_path              AS _source_file,
  _metadata.file_modification_time AS _source_file_modified_at
FROM STREAM read_files(
  '/Volumes/workspace/raw_data/raw_sdp/customers',
  format              => 'json',
  schemaHints         => 'customer_id STRING, name STRING, email STRING, membership_level STRING, region STRING',
  schemaEvolutionMode => 'rescue'
);
