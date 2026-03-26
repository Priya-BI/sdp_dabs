-- =============================================================================
-- SILVER: orders
-- Source : workspace.bronze_sdp_dev.bronze_orders (streaming)
-- Notes  : Normalise status to UPPER, round amounts, derive order_year/month
--          for analytics convenience, add is_valid. No deduplication.
--          CDF enabled for gold AUTO CDC (SCD Type 1).
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.silver_sdp_dev.silver_orders
COMMENT "Cleaned and enriched order records. No deduplication at this layer."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality'                    = 'silver'
)
CLUSTER BY (order_id)
AS
SELECT
  order_id,
  customer_id,
  order_date,
  YEAR(order_date)               AS order_year,
  MONTH(order_date)              AS order_month,
  ROUND(total_amount, 2)         AS total_amount,
  UPPER(TRIM(status))            AS status,
  CASE
    WHEN order_id IS NULL                               THEN false
    WHEN customer_id IS NULL                            THEN false
    WHEN order_date IS NULL                             THEN false
    WHEN total_amount IS NULL OR total_amount < 0       THEN false
    ELSE true
  END                            AS is_valid,
  _ingested_at,
  _source_file
FROM STREAM(workspace.bronze_sdp_dev.bronze_orders)
WHERE order_id IS NOT NULL;
