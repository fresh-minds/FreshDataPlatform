# Parallel dbt Transformations

This folder is a parallel implementation of the existing PySpark transformations in `pipelines/`,
plus the SQL-native transformation layer for ingestion sources.

## What this project does

- Recreates SQL-native transformations for the canonical `odp_staffing_demand` domain.
- During migration, canonical bronze/silver shims in `odp_staffing_demand` `ref()` transitional legacy `odp_staffing_demand` models.
- Provides medallion dbt models for ingestion sources:
  - bronze (`brz_*`) source-aligned normalization
  - silver (`slv_*`) enrichment
  - gold (`dim_*` / `fct_*`) serving models
- Keeps model outputs aligned with the current warehouse-facing table names.
- Supports dbt snapshots when SCD2 history tables are enabled.
- Supports two execution modes:
  - **Production mode** (`use_seed_data: false`): reads from warehouse source tables.
  - **Local verification mode** (`use_seed_data: true`): reads from dbt seeds in `seeds/`.

## Project layout

```text
dbt/
├── models/
│   ├── bronze/
│   │   └── <source>/               brz_<source>__<dataset> (view)
│   │   └── odp_staffing_demand/    canonical shim views -> legacy brz_odp_staffing_demand__*
│   ├── silver/
│   │   └── <source>/               slv_<source>__<dataset>_enriched (view)
│   │   └── odp_staffing_demand/    canonical shim views -> legacy slv_odp_staffing_demand__*
│   ├── gold/
│   │   └── <source>/               dim_* and fct_<dataset> models
├── _model_templates/               Templates for adding new sources
│   ├── bronze/                     brz + source YAML templates
│   ├── silver/                     slv enriched template
│   ├── gold/                       dim + fct + YAML templates
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
.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt

# 3) Build complete parallel graph using seed data
.venv/bin/dbt seed --project-dir dbt --profiles-dir dbt --full-refresh
.venv/bin/dbt run --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
.venv/bin/dbt snapshot --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
```

### Run ingestion source models only

```bash
# Example: run + test one source graph (bronze through gold)
.venv/bin/dbt run  --project-dir dbt --profiles-dir dbt --select brz_<source>__<dataset>+
.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --select brz_<source>__<dataset>+
```

## Adding models for a new ingestion source

Templates for bronze, silver, and gold models live in `_model_templates/`
(kept outside `models/` so dbt does not compile them).

```bash
# Copy and rename templates for your source
cp _model_templates/bronze/brz_SOURCENAME__DATASET.sql \
   models/bronze/<source>/brz_<source>__<dataset>.sql
# Repeat for silver and gold — see docs/INGESTION_GUIDE.md for details
```

Full instructions: [Data Ingestion Guide](../docs/INGESTION_GUIDE.md)

## Package management

Run `dbt deps` before `dbt run` if packages are not already installed. The
`packages.yml` file pins `dbt_utils` to `1.3.3` to keep dbt 1.9 compatible.

## dbt docs + lineage UI

Generate dbt docs artifacts from all dbt logic:

```bash
.venv/bin/dbt deps --project-dir dbt --profiles-dir dbt
.venv/bin/dbt docs generate --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
```

From repo root, you can also run:

```bash
make dbt-docs-refresh
```

This regenerates docs and serves them at `http://localhost:8089` through the
`dbt-docs` static service.

## Mapping

Detailed transformation mapping lives in `dbt/TRANSFORMATION_MAPPING.md`.
