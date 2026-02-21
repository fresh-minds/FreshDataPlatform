# Parallel dbt Transformations

This folder is a parallel implementation of the existing PySpark transformations in `pipelines/`,
plus the SQL-native transformation layer for ingestion sources.

## What this project does

- Recreates SQL-native transformations for `job_market_nl`.
- Provides staging, intermediate, and dimensional mart models for ingestion
  sources (e.g. `source_sp1`).
- Keeps model outputs aligned with the current warehouse-facing table names.
- Supports dbt snapshots when SCD2 history tables are enabled.
- Supports two execution modes:
  - **Production mode** (`use_seed_data: false`): reads from warehouse source tables.
  - **Local verification mode** (`use_seed_data: true`): reads from dbt seeds in `seeds/`.

## Project layout

```text
dbt_parallel/
├── models/
│   ├── staging/
│   │   └── source_sp1/             stg_source_sp1__vacatures (view)
│   ├── intermediate/
│   │   └── source_sp1/             int_source_sp1__vacatures_enriched (view)
│   └── marts/
│       └── source_sp1/             dim_client, dim_location, dim_category,
│                                   dim_vacancy_status, fct_vacatures (views)
├── _model_templates/               Templates for adding new sources
│   ├── staging/                    stg + source YAML templates
│   ├── intermediate/               int enriched template
│   └── marts/                      dim + fct + YAML templates
├── seeds/                          Seed CSVs for local verification
├── snapshots/                      SCD2 snapshot definitions
└── profiles.yml                    Connection profiles (local + CI)
```

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

### Run ingestion source models only

```bash
# Run + test all source_sp1 models (staging through marts)
.venv/bin/dbt run  --project-dir dbt_parallel --profiles-dir dbt_parallel --select stg_source_sp1__vacatures+
.venv/bin/dbt test --project-dir dbt_parallel --profiles-dir dbt_parallel --select stg_source_sp1__vacatures+
```

## Adding models for a new ingestion source

Templates for staging, intermediate, and mart models live in `_model_templates/`
(kept outside `models/` so dbt does not compile them).

```bash
# Copy and rename templates for your source
cp _model_templates/staging/stg_SOURCENAME__DATASET.sql \
   models/staging/<source>/stg_<source>__<dataset>.sql
# Repeat for intermediate and marts — see docs/INGESTION_GUIDE.md for details
```

Full instructions: [Data Ingestion Guide](../docs/INGESTION_GUIDE.md)

## Package management

Run `dbt deps` before `dbt run` if packages are not already installed. The
`packages.yml` file pins `dbt_utils` to `1.3.3` to keep dbt 1.9 compatible.

## Mapping

Detailed transformation mapping lives in `dbt_parallel/TRANSFORMATION_MAPPING.md`.
