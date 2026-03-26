# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Workspace

- **Databricks host**: `https://dbc-e2854db0-3829.cloud.databricks.com`
- **Auth**: `~/.databrickscfg` profile `DEFAULT` (Databricks OAuth, no PAT needed)
- **MCP server**: configured in `.mcp.json` — Databricks MCP tools are available when the server is running

## Common commands

All commands run from inside `sdp_medallion/`.

```bash
# Validate bundle config (catches YAML/variable errors before deploying)
databricks bundle validate

# Deploy to dev (default target — prefixes pipeline name with [dev username])
databricks bundle deploy

# Run the full pipeline
databricks bundle run sdp_medallion_etl

# Run a single flow by table name
databricks bundle run sdp_medallion_etl --select bronze_customers

# Deploy and run against prod
databricks bundle deploy --target prod
databricks bundle run sdp_medallion_etl --target prod

# Inspect a deployed pipeline (state, last update, errors)
databricks pipelines get <pipeline_id> -o json

# List tables in a layer schema
databricks tables list workspace bronze_sdp_dev
databricks tables list workspace silver_sdp_dev
databricks tables list workspace gold_sdp_dev

# Browse raw source files
databricks fs ls dbfs:/Volumes/workspace/raw_data/raw_sdp
```

## Architecture

### Overview

A single **Serverless Lakeflow Spark Declarative Pipeline** (`sdp_medallion_etl`) written entirely in SQL, managed via Databricks Asset Bundles. It reads raw JSON from a Unity Catalog volume and materialises three separate UC schemas (bronze → silver → gold) in the `workspace` catalog. SDP automatically resolves the dependency order across all layers in one pipeline run.

### Schema separation

The pipeline default catalog/schema is set to bronze (`workspace.bronze_sdp_dev`), so unqualified table names in bronze SQL land there automatically. Silver and gold SQL files use **fully-qualified three-part names** (`workspace.silver_sdp_dev.*`, `workspace.gold_sdp_dev.*`) because SQL files cannot interpolate DABs variables. The `configuration:` block in the pipeline YAML passes all three schema names as Spark conf keys for reference.

Schemas are parameterised as DABs variables (`bronze_schema`, `silver_schema`, `gold_schema`) in `databricks.yml`, with separate values for `dev` and `prod` targets.

### Data flow

```
/Volumes/workspace/raw_data/raw_sdp/
  customers/   orders/   line_items/
        ↓  STREAM read_files() + schemaEvolutionMode=rescue
  workspace.bronze_sdp_dev.*   (STREAMING_TABLE, CDF=true, columnMapping=name)
        ↓  STREAM()
  workspace.silver_sdp_dev.*   (STREAMING_TABLE, CDF=true, cleaned + enriched)
        ↓  APPLY CHANGES INTO (AUTO CDC)
  workspace.gold_sdp_dev.dim_customers     (SCD Type 2 — small table)
  workspace.gold_sdp_dev.fact_orders       (SCD Type 1 — larger table)
  workspace.gold_sdp_dev.fact_line_items   (SCD Type 1 — larger table)
        ↓  JOIN
  workspace.gold_sdp_dev.gold_sales_by_region_monthly   (MATERIALIZED_VIEW)
  workspace.gold_sdp_dev.gold_product_sales_metrics     (MATERIALIZED_VIEW)
```

### Key design decisions

**Bronze** — `CREATE OR REFRESH STREAMING TABLE` (unqualified name → default bronze schema). Every table has `delta.enableChangeDataFeed = true` (required for silver to stream changes) and `delta.columnMapping.mode = name` (supports additive column evolution). `schemaEvolutionMode => 'rescue'` stores unexpected fields in `_rescued_data`. Metadata columns `_ingested_at`, `_source_file`, `_source_file_modified_at` are appended in every SELECT.

**Silver** — Streaming tables reading from bronze via `FROM STREAM(workspace.bronze_sdp_dev.*)`. Applies cleaning (trim, lowercase, UPPER normalisation) and enrichment (`line_total`, `order_year`/`order_month`, `is_valid` flag) without deduplication. CDF enabled for downstream AUTO CDC.

**Gold SCD** — `APPLY CHANGES INTO` (AUTO CDC) deduplicates on primary key using `SEQUENCE BY _ingested_at`. Pipeline metadata columns are excluded from COLUMNS to prevent spurious history rows: `COLUMNS * EXCEPT (_ingested_at, _source_file)`. `dim_customers` uses `SCD TYPE 2` (history kept; query `WHERE __END_AT IS NULL` for current rows). `fact_orders` and `fact_line_items` use `SCD TYPE 1` (latest value wins, no history).

**Gold aggregates** — `CREATE OR REFRESH MATERIALIZED VIEW`. `gold_sales_by_region_monthly` joins `fact_orders × dim_customers` filtering `c.__END_AT IS NULL`. `gold_product_sales_metrics` joins `fact_line_items × fact_orders`.

### Adding a new layer or table

1. Create a `.sql` file in the appropriate `transformations/<layer>/` directory.
2. Bronze: use unqualified `CREATE OR REFRESH STREAMING TABLE <name>` — it goes to the default bronze schema.
3. Silver/gold: use fully-qualified `CREATE OR REFRESH STREAMING TABLE workspace.<schema>.<name>`.
4. For AUTO CDC targets, declare the empty table first, then `APPLY CHANGES INTO` in the same file.
5. `databricks bundle validate && databricks bundle deploy && databricks bundle run sdp_medallion_etl`

### Schema changes in bronze

Streaming tables require a **full refresh** for incompatible schema changes (e.g. renaming a column). Compatible changes (new columns) are handled automatically via `columnMapping.mode = name`. To force a full refresh:
```bash
databricks bundle run sdp_medallion_etl --full-refresh-all
# or for a single table:
databricks bundle run sdp_medallion_etl --full-refresh bronze_customers
```
