-- =============================================================================
-- SILVER: customers
-- Source : workspace.bronze_sdp_dev.bronze_customers (streaming)
-- Notes  : Trim/normalise strings, lowercase email, add is_valid flag.
--          No deduplication — that happens downstream via AUTO CDC SCD Type 2.
--          CDF enabled so gold AUTO CDC can stream changes.
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.silver_sdp_dev.silver_customers
COMMENT "Cleaned and enriched customer records. No deduplication at this layer."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality'                    = 'silver'
)
CLUSTER BY (customer_id)
AS
SELECT
  customer_id,
  TRIM(name)               AS name,
  LOWER(TRIM(email))       AS email,
  TRIM(membership_level)   AS membership_level,
  TRIM(region)             AS region,
  CASE
    WHEN customer_id IS NULL          THEN false
    WHEN email IS NULL
      OR TRIM(email) = ''             THEN false
    WHEN name IS NULL
      OR TRIM(name) = ''              THEN false
    ELSE true
  END                      AS is_valid,
  _ingested_at,
  _source_file
FROM STREAM(workspace.bronze_sdp_dev.bronze_customers)
WHERE customer_id IS NOT NULL;
