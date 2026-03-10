---
title: Ingestion Guide
description: Step-by-step guide for adding new data sources to the platform.
order: 5
---

This guide walks you through adding a new data source to the ingestion platform. By following these steps you will create a complete pipeline that:

1. **Extracts** raw data into MinIO bronze (medallion architecture)
2. **Parses** and **loads** structured records into a Postgres silver table
3. **Transforms** the data through dbt bronze → silver → gold
4. **Observes** the pipeline with metrics and a success gate
5. **Publishes** run, artifact, lineage, and quality metadata

## Architecture Overview

```
┌────────────┐   Playwright / API   ┌──────────────┐
│   Source    │ ──────────────────▶  │ MinIO Bronze │
│  (website, │   raw JSON/HTML/CSV  │  (lakehouse) │
│   API, …)  │                      └──────┬───────┘
└────────────┘                             │ parse
                                           ▼
                                    ┌──────────────┐
                                    │   Postgres   │
                                    │   Silver     │
                                    └──────┬───────┘
                                           │ dbt
                        ┌──────────────────┼──────────────────┐
                        ▼                  ▼                  ▼
                  ┌───────────┐    ┌──────────────┐    ┌───────────┐
                  │ brz_ view │ →  │ slv_ enriched│ →  │ dim_ / fct│
                  │  (bronze) │    │   (silver)   │    │  (gold)   │
                  └───────────┘    └──────────────┘    └───────────┘
```

### Naming Conventions

| Layer | Prefix | Example |
|-------|--------|---------|
| Bronze | `brz_` | `brz_acme_portal__orders` |
| Silver | `slv_` | `slv_acme_portal__orders_enriched` |
| Gold Dimension | `dim_` | `dim_customer` |
| Gold Fact | `fct_` | `fct_orders` |

## Step-by-Step Instructions

### Step 1 — Define the Source Table Config

Copy the template and define your columns:

```bash
cp -r src/ingestion/_template src/ingestion/<source_name>
```

Edit `src/ingestion/<source_name>/config.py` with your `SourceTableConfig` including columns, primary keys, and indexes.

### Step 2 — Write the Extractor

The extractor downloads raw data from the source. It supports both browser-based (Playwright) and API-based (requests) extraction methods.

### Step 3 — Write the Parser

The parser transforms raw bytes into flat dictionaries matching your `SourceTableConfig` columns. Record keys must exactly match column names.

### Step 4 — Create dbt Models

Create bronze, silver, and gold dbt models:

```bash
mkdir -p dbt/models/bronze/<source_name>
mkdir -p dbt/models/silver/<source_name>
mkdir -p dbt/models/gold/<source_name>
```

- **Bronze**: 1:1 view over source table with light type-casting
- **Silver**: computed fields, NULL coalescing, business logic
- **Gold**: dimensions (surrogate keys) and facts (joins to dimensions)

### Step 5 — Create the Airflow DAG

```bash
cp dags/_template_dag.py dags/<source_name>_<dataset>_ingestion.py
```

Wire your config, extractor, parser, and dbt selector into the DAG template.

### Step 6 — Test Locally

```bash
# Run dbt
dbt run --select brz_<source_name>__<dataset>+
dbt test --select brz_<source_name>__<dataset>+

# Trigger the full DAG
airflow dags trigger <source_name>_<dataset>_ingestion
```

## Checklist

- [ ] `SourceTableConfig` defined
- [ ] Extractor implemented
- [ ] Parser implemented
- [ ] dbt bronze/silver/gold models created
- [ ] Airflow DAG wired
- [ ] DDL verified locally
- [ ] dbt run + test passes
- [ ] End-to-end DAG trigger successful
- [ ] Metadata rows written to `platform_metadata`
