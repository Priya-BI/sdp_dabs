-- =============================================================================
-- GOLD AGGREGATE 1: sales by region & membership tier, by month
-- Sources: fact_orders (SCD1) JOIN dim_customers (SCD2, current rows only)
-- Metrics: order count, unique customers, total/avg/min/max revenue,
--          revenue split by order status (delivered vs shipped vs other).
-- Use case: regional performance dashboards, tier-level revenue tracking.
-- =============================================================================
CREATE OR REFRESH MATERIALIZED VIEW workspace.gold_sdp_dev.gold_sales_by_region_monthly
COMMENT "Monthly revenue and order metrics by customer region and membership tier. Join of fact_orders × dim_customers (current rows)."
TBLPROPERTIES ('quality' = 'gold')
AS
SELECT
  c.region,
  c.membership_level,
  DATE_TRUNC('MONTH', o.order_date)                                             AS order_month,

  -- Volume
  COUNT(DISTINCT o.order_id)                                                    AS order_count,
  COUNT(DISTINCT o.customer_id)                                                 AS unique_customers,

  -- Revenue
  ROUND(SUM(o.total_amount), 2)                                                 AS total_revenue,
  ROUND(AVG(o.total_amount), 2)                                                 AS avg_order_value,
  MAX(o.total_amount)                                                            AS max_order_value,
  MIN(o.total_amount)                                                            AS min_order_value,

  -- Revenue by status
  ROUND(SUM(CASE WHEN o.status = 'DELIVERED' THEN o.total_amount ELSE 0 END), 2) AS delivered_revenue,
  ROUND(SUM(CASE WHEN o.status = 'SHIPPED'   THEN o.total_amount ELSE 0 END), 2) AS shipped_revenue,
  ROUND(SUM(CASE WHEN o.status NOT IN ('DELIVERED','SHIPPED') THEN o.total_amount ELSE 0 END), 2) AS other_revenue,

  -- Order counts by status
  COUNT(CASE WHEN o.status = 'DELIVERED' THEN 1 END)                           AS delivered_order_count,
  COUNT(CASE WHEN o.status = 'SHIPPED'   THEN 1 END)                           AS shipped_order_count,
  COUNT(CASE WHEN o.status NOT IN ('DELIVERED','SHIPPED') THEN 1 END)          AS other_order_count

FROM workspace.gold_sdp_dev.fact_orders o
JOIN workspace.gold_sdp_dev.dim_customers c
  ON o.customer_id = c.customer_id
 AND c.__END_AT IS NULL          -- current version of each customer (SCD Type 2)
WHERE o.is_valid = true
GROUP BY
  c.region,
  c.membership_level,
  DATE_TRUNC('MONTH', o.order_date);
