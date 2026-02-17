# Parallel dbt Transformations

This folder is a parallel implementation of the existing PySpark transformations in `pipelines/`.

## What this project does

- Recreates the medallion transformations for `finance` and `job_market_nl`.
- Keeps model outputs aligned with the current warehouse-facing table names.
- Supports dbt snapshots when SCD2 history tables are enabled.
- Supports two execution modes:
  - **Production mode** (`use_seed_data: false`): reads from warehouse source tables.
  - **Local verification mode** (`use_seed_data: true`): reads from dbt seeds in `seeds/`.

## Quick start

From repo root:

```bash
# 1) Ensure warehouse Postgres is running
docker compose up -d warehouse

# 2) Validate connection
.venv/bin/dbt debug --project-dir dbt_parallel --profiles-dir dbt_parallel

# 3) Build complete parallel graph using seed data
.venv/bin/dbt seed --project-dir dbt_parallel --profiles-dir dbt_parallel --full-refresh
.venv/bin/dbt run --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'
.venv/bin/dbt snapshot --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'
.venv/bin/dbt test --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'
```

## Mapping

Detailed transformation mapping lives in `dbt_parallel/TRANSFORMATION_MAPPING.md`.
