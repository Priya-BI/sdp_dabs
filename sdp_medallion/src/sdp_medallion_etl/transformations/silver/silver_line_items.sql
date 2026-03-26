-- =============================================================================
-- SILVER: line_items
-- Source : workspace.bronze_sdp_dev.bronze_line_items (streaming)
-- Notes  : Trim product_name, round prices, compute line_total (qty × price),
--          add is_valid. No deduplication.
--          CDF enabled for gold AUTO CDC (SCD Type 1).
-- =============================================================================
CREATE OR REFRESH STREAMING TABLE workspace.silver_sdp_dev.silver_line_items
COMMENT "Cleaned and enriched line item records with computed line total. No deduplication at this layer."
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'quality'                    = 'silver'
)
CLUSTER BY (line_item_id)
AS
SELECT
  line_item_id,
  order_id,
  TRIM(product_name)                        AS product_name,
  quantity,
  ROUND(unit_price, 2)                      AS unit_price,
  ROUND(quantity * unit_price, 2)           AS line_total,
  CASE
    WHEN line_item_id IS NULL                         THEN false
    WHEN order_id IS NULL                             THEN false
    WHEN product_name IS NULL
      OR TRIM(product_name) = ''                      THEN false
    WHEN quantity IS NULL OR quantity <= 0            THEN false
    WHEN unit_price IS NULL OR unit_price < 0         THEN false
    ELSE true
  END                                       AS is_valid,
  _ingested_at,
  _source_file
FROM STREAM(workspace.bronze_sdp_dev.bronze_line_items)
WHERE line_item_id IS NOT NULL;
