-- =============================================================================
-- GOLD AGGREGATE 2: product-level sales metrics
-- Sources: fact_line_items (SCD1) JOIN fact_orders (SCD1)
-- Metrics: units sold, revenue, avg/min/max price, avg qty per order,
--          first/last sold date, # distinct orders.
-- Use case: product performance analysis, pricing strategy, inventory planning.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW workspace.gold_sdp_dev.gold_product_sales_metrics
COMMENT "Product-level sales performance metrics. Aggregates fact_line_items joined to fact_orders for order context."
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  li.product_name,

  -- Reach
  COUNT(DISTINCT li.order_id)              AS distinct_orders,
  COUNT(li.line_item_id)                   AS total_line_items,

  -- Volume
  SUM(li.quantity)                         AS total_units_sold,
  ROUND(AVG(li.quantity), 2)               AS avg_qty_per_order,

  -- Revenue
  ROUND(SUM(li.line_total), 2)             AS total_revenue,
  ROUND(AVG(li.line_total), 2)             AS avg_line_total,

  -- Pricing
  ROUND(AVG(li.unit_price), 2)             AS avg_unit_price,
  MIN(li.unit_price)                       AS min_unit_price,
  MAX(li.unit_price)                       AS max_unit_price,

  -- Time range
  MIN(o.order_date)                        AS first_sold_date,
  MAX(o.order_date)                        AS last_sold_date,

  -- Status breakdown (order-level)
  COUNT(DISTINCT CASE WHEN o.status = 'DELIVERED' THEN li.order_id END) AS delivered_order_count,
  COUNT(DISTINCT CASE WHEN o.status = 'SHIPPED'   THEN li.order_id END) AS shipped_order_count

FROM workspace.gold_sdp_dev.fact_line_items li
JOIN workspace.gold_sdp_dev.fact_orders o
  ON li.order_id = o.order_id
WHERE li.is_valid = true
  AND o.is_valid  = true
GROUP BY li.product_name;
