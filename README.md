# dabs_sdp_cl — Medallion Pipeline (SDP)

Serverless Lakeflow Spark Declarative Pipeline implementing a full Bronze → Silver → Gold medallion architecture over raw e-commerce JSON data. Built with Databricks Asset Bundles (DABs) using SQL throughout.

- **Workspace**: `https://dbc-e2854db0-3829.cloud.databricks.com`
- **Catalog**: `workspace`
- **Source volume**: `/Volumes/workspace/raw_data/raw_sdp`

---

## Project structure

```
dabs_sdp_cl/
└── sdp_medallion/                          # DABs bundle root
    ├── databricks.yml                      # Bundle config — variables, dev/prod targets
    ├── resources/
    │   └── sdp_medallion_etl.pipeline.yml  # Pipeline resource (serverless, schema config)
    └── src/sdp_medallion_etl/
        ├── explorations/                   # Ad-hoc notebooks
        └── transformations/
            ├── bronze/                     # Raw ingestion — streaming tables
            ├── silver/                     # Cleaned & enriched — streaming tables
            └── gold/                       # SCD tables + aggregate materialized views
```

---

## Data model

### Source data (`workspace.raw_data.raw_sdp`)

| Entity | Rows | Primary key |
|---|---|---|
| `customers` | 100 | `customer_id` |
| `orders` | 500 | `order_id` |
| `line_items` | 1,500 | `line_item_id` |

### Pipeline outputs

| Layer | Schema | Tables | Type |
|---|---|---|---|
| Bronze | `workspace.bronze_sdp_dev` | `bronze_customers`, `bronze_orders`, `bronze_line_items` | STREAMING_TABLE |
| Silver | `workspace.silver_sdp_dev` | `silver_customers`, `silver_orders`, `silver_line_items` | STREAMING_TABLE |
| Gold | `workspace.gold_sdp_dev` | `dim_customers` (SCD2), `fact_orders` (SCD1), `fact_line_items` (SCD1) | STREAMING_TABLE |
| Gold | `workspace.gold_sdp_dev` | `gold_sales_by_region_monthly`, `gold_product_sales_metrics` | MATERIALIZED_VIEW |

---

## Layer summary

**Bronze** — ingests JSON with `STREAM read_files()`, schema evolution via rescue mode, appends `_ingested_at`, `_source_file`, `_source_file_modified_at`. Change Data Feed enabled on all tables.

**Silver** — streams from bronze, applies cleaning (trim, normalise case, round amounts), derives `line_total`, `order_year`/`order_month`, and adds an `is_valid` flag. No deduplication at this layer.

**Gold (SCD)** — AUTO CDC deduplicates on primary key using `SEQUENCE BY _ingested_at`. `dim_customers` tracks full history (SCD Type 2); query `WHERE __END_AT IS NULL` for current records. `fact_orders` and `fact_line_items` keep only the latest value per key (SCD Type 1).

**Gold (aggregates)** — two materialized views: regional monthly revenue metrics (region × membership tier × month) and product-level sales performance (units, revenue, pricing, order reach).

---

## Deploy & run

All commands run from `sdp_medallion/`.

```bash
# Validate config
databricks bundle validate

# Deploy to dev (default)
databricks bundle deploy

# Run the pipeline
databricks bundle run sdp_medallion_etl

# Run a single table
databricks bundle run sdp_medallion_etl --select bronze_customers

# Full refresh (e.g. after incompatible schema change)
databricks bundle run sdp_medallion_etl --full-refresh-all

# Deploy and run in production
databricks bundle deploy --target prod
databricks bundle run sdp_medallion_etl --target prod
```

In dev mode the pipeline is deployed as `[dev priya_ambastha] sdp_medallion_etl` and writes to the `*_sdp_dev` schemas. In prod mode it writes to `bronze_sdp`, `silver_sdp`, `gold_sdp`.

---

## References

- [Lakeflow Spark Declarative Pipelines](https://docs.databricks.com/aws/en/ldp/)
- [AUTO CDC / SCD](https://docs.databricks.com/aws/en/ldp/cdc)
- [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
