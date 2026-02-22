# Data Ingestion Guide — End-to-End

This guide walks you through adding a **new data source** to the ingestion
platform.  By following these steps you will create a complete pipeline that:

1. **Extracts** raw data into MinIO bronze (medallion architecture).
2. **Parses** and **loads** structured records into a Postgres silver table.
3. **Transforms** the data through dbt staging → intermediate → dimensional
   marts (facts + dimensions).
4. **Observes** the pipeline with metrics and a success gate.

> **Time estimate:** 2–4 hours for a straightforward API/scrape source.

---

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
                                    │ (schema.table)│
                                    └──────┬───────┘
                                           │ dbt
                        ┌──────────────────┼──────────────────┐
                        ▼                  ▼                  ▼
                  ┌───────────┐    ┌──────────────┐    ┌───────────┐
                  │ stg_ view │ →  │ int_ enriched│ →  │ dim_ / fct│
                  │ (staging) │    │(intermediate)│    │  (marts)  │
                  └───────────┘    └──────────────┘    └───────────┘
```

### Naming Conventions

| Layer          | Prefix    | Example                                    |
|----------------|-----------|--------------------------------------------|
| Staging        | `stg_`    | `stg_source_sp1__vacatures`       |
| Intermediate   | `int_`    | `int_source_sp1__vacatures_enriched` |
| Dimension      | `dim_`    | `dim_client`                               |
| Fact           | `fct_`    | `fct_vacatures`                            |

### File Layout

```
project/
├── dags/
│   ├── _template_dag.py                        ← DAG template
│   └── <source>_<dataset>_ingestion.py         ← your DAG
├── src/ingestion/
│   ├── common/
│   │   ├── source_config.py                    ← Column + SourceTableConfig
│   │   ├── postgres.py                         ← generic DDL + upsert
│   │   ├── dag_helpers.py                      ← reusable DAG callables
│   │   ├── minio.py                            ← MinIO read/write helpers
│   │   └── provenance.py                       ← artifact metadata builder
│   ├── _template/                              ← Python templates
│   │   ├── config.py
│   │   ├── extract.py
│   │   └── parse.py
│   └── <source>/                               ← your source module
│       ├── __init__.py
│       ├── config.py
│       ├── extract_<dataset>.py
│       └── parse_<dataset>.py
├── dbt_parallel/
│   ├── _model_templates/                       ← dbt model templates (outside models/)
│   │   ├── staging/
│   │   ├── intermediate/
│   │   └── marts/
│   └── models/
│       ├── staging/<source>/
│       ├── intermediate/<source>/
│       └── marts/<source>/
```

---

## Step-by-Step Instructions

### Step 0 — Prerequisites

- Docker Compose stack running (`docker compose up -d`)
- Airflow, MinIO, Postgres, dbt all healthy
- Env vars configured in `.env` (see `.env.example`)

---

### Step 1 — Define the Source Table Config

Copy the template:

```bash
cp -r src/ingestion/_template src/ingestion/<source_name>
```

Edit `src/ingestion/<source_name>/config.py`:

```python
from src.ingestion.common.source_config import Column, SourceTableConfig

MY_CONFIG = SourceTableConfig(
    source_name="<source_name>",          # snake_case
    dataset="<dataset>",                  # e.g. "orders", "vacatures"
    schema="<source_name>",               # Postgres schema
    table="<dataset>",                    # Postgres table
    columns=[
        Column("entity_id",        "text",     primary_key=True),
        Column("title",            "text"),
        Column("status",           "text",     index=True),
        Column("created_date",     "date",     index=True),
        Column("amount",           "numeric"),
        Column("description",      "text"),
        Column("source_url",       "text"),
        Column("bronze_object_path","text"),
        Column("checksum_sha256",  "text"),
        Column("ingested_at",      "timestamp without time zone", not_null=True),
    ],
    checksum_column="checksum_sha256",
    ingested_at_column="ingested_at",
)
```

**Rules:**
- First `primary_key=True` column becomes the PK constraint.
- Add `index=True` to columns you filter/sort on.
- Always include `checksum_sha256` and `ingested_at`.
- Use snake_case for all column names.
- Supported Postgres types: `text`, `date`, `numeric`, `boolean`, `integer`,
  `timestamp without time zone`, `jsonb`.

---

### Step 2 — Write the Extractor

Edit `src/ingestion/<source_name>/extract_<dataset>.py`:

The extractor is responsible for downloading raw data from the source. It
returns a list of `RawArtifact` objects.

**Browser-based extraction** (Playwright):

```python
def extract_all(*, page, base_url, run_id, run_dt, lookback_days=7):
    page.goto(f"{base_url}/data-page")
    page.wait_for_selector("table.results")
    # ... navigate, paginate, capture XHR responses or DOM HTML
    return [RawArtifact(
        artifact_id=f"xhr_{run_id}_0000",
        raw_bytes=response_bytes,
        url=page.url,
        content_type="application/json",
        extraction_method="xhr_json",
    )]
```

**API-based extraction** (requests):

```python
def extract_all(*, api_key, run_id, run_dt, **kwargs):
    resp = requests.get("https://api.example.com/data", headers={"Authorization": f"Bearer {api_key}"})
    resp.raise_for_status()
    return [RawArtifact(
        artifact_id=f"api_{run_id}_0000",
        raw_bytes=resp.content,
        url=resp.url,
        content_type="application/json",
        extraction_method="api_json",
    )]
```

**Supported extraction methods:** `api_json`, `xhr_json`, `dom_html`,
`export_download`, `dom_html_snapshot`.

---

### Step 3 — Write the Parser

Edit `src/ingestion/<source_name>/parse_<dataset>.py`:

The parser transforms raw bytes into flat dictionaries matching your
`SourceTableConfig` columns.

```python
def parse_artifacts(artifacts: list[dict]) -> list[dict]:
    records = []
    for art in artifacts:
        data = json.loads(art["raw_bytes"])
        for item in data["results"]:
            records.append({
                "entity_id": item["id"],
                "title": item["name"],
                "status": item.get("status", "unknown"),
                "created_date": item.get("created_at"),
                "amount": item.get("total"),
                "description": item.get("description", ""),
                "source_url": art.get("url", ""),
                "bronze_object_path": art["bronze_object_path"],
                "checksum_sha256": _compute_checksum(record),
            })
    return records
```

**Important:**
- Record keys must exactly match the column names in your `SourceTableConfig`.
- The `checksum_sha256` should be computed over all data fields (excluding
  `ingested_at` and `bronze_object_path`) to enable change detection.
- Do NOT include `ingested_at` — it defaults to `now()`.

---

### Step 4 — Create dbt Models

#### Step 4a — Staging Model

Create `dbt_parallel/models/staging/<source_name>/`:

```bash
mkdir -p dbt_parallel/models/staging/<source_name>
```

Copy the templates:

```bash
cp dbt_parallel/_model_templates/staging/stg_SOURCENAME__DATASET.sql \
   dbt_parallel/models/staging/<source_name>/stg_<source_name>__<dataset>.sql

cp dbt_parallel/_model_templates/staging/_stg_SOURCENAME__models.yml \
   dbt_parallel/models/staging/<source_name>/_stg_<source_name>__models.yml
```

Edit the staging model — it should be a 1:1 view over the source table with
light type-casting and renaming only. No joins or business logic.

Edit the YAML — register the source and model, add `not_null` + `unique` tests
on the primary key.

#### Step 4b — Intermediate Model

Create `dbt_parallel/models/intermediate/<source_name>/`:

```bash
mkdir -p dbt_parallel/models/intermediate/<source_name>
```

The intermediate model adds computed fields, coalesces NULLs for dimension
keys, and applies business logic. Reference the staging model with `{{ ref() }}`.

Example computed fields:
- `days_since_created` = `current_date - created_date`
- `is_expired` = `closing_date < current_date`
- `status_clean` = `coalesce(nullif(trim(status), ''), 'Unknown')`

#### Step 4c — Marts (Dimensions + Facts)

Create `dbt_parallel/models/marts/<source_name>/`:

```bash
mkdir -p dbt_parallel/models/marts/<source_name>
```

**Dimensions** — `SELECT DISTINCT` the natural key from the enriched
intermediate model, with a `{{ hashed_key(['column_name']) }}` surrogate key:

```sql
with source as (
    select distinct status_clean as status_name
    from {{ ref('int_<source_name>__<dataset>_enriched') }}
)
select
    {{ hashed_key(['status_name']) }} as status_sk,
    status_name
from source
```

**Fact** — join the enriched model to all dimension tables via surrogate keys:

```sql
with enriched as (
    select * from {{ ref('int_<source_name>__<dataset>_enriched') }}
),
dim_status as (
    select * from {{ ref('dim_status') }}
)
select
    dim_status.status_sk,
    enriched.entity_id,
    enriched.amount,
    enriched.ingested_at
from enriched
left join dim_status
    on enriched.status_clean = dim_status.status_name
```

**YAML** — add `not_null`/`unique` tests on all surrogate keys, and
`relationships` tests on fact table foreign keys:

```yaml
columns:
  - name: status_sk
    tests:
      - not_null
      - relationships:
          to: ref('dim_status')
          field: status_sk
```

---

### Step 5 — Create the Airflow DAG

Copy the template:

```bash
cp dags/_template_dag.py dags/<source_name>_<dataset>_ingestion.py
```

Edit the file:

1. Update `_SOURCE_NAME` and `_DATASET` constants.
2. Update `_DAG_ID` to your real DAG ID (for example: `<source_name>_<dataset>_ingestion`).
3. Import your source config.
4. Wire `make_ensure_ddl_callable(YOUR_CONFIG)` into preflight.
5. Implement `_extract_to_bronze()` using your extractor.
6. Implement `_parse_and_load()` using your parser + `upsert_records()`.
7. Set the dbt selector in `make_dbt_run_callable(select="stg_<source>__<dataset>+")`.
8. Add source-specific connections to `make_validate_connections_callable()`.

**Key imports from `dag_helpers`:**

```python
from src.ingestion.common.dag_helpers import (
    make_default_args,
    make_dbt_run_callable,
    make_emit_metrics_callable,
    make_ensure_bucket_callable,
    make_ensure_ddl_callable,
    make_validate_connections_callable,
    mime_to_ext,
    try_get_conn,
)
```

---

### Step 6 — Test Locally

#### 6a. Verify DDL generation

```bash
docker exec -it airflow-worker python3 -c "
from src.ingestion.<source_name>.config import MY_CONFIG
from src.ingestion.common.postgres import _generate_create_table_sql, _generate_index_sql
print(_generate_create_table_sql(MY_CONFIG))
print(_generate_index_sql(MY_CONFIG))
"
```

#### 6b. Run parse_load manually

```bash
docker exec -it airflow-worker python3 -c "
from src.ingestion.common.postgres import ensure_source_ddl, ensure_state_table, upsert_records
from src.ingestion.<source_name>.config import MY_CONFIG
ensure_source_ddl(MY_CONFIG)
ensure_state_table()
print('DDL created for', MY_CONFIG.fqn)
"
```

#### 6c. Run dbt

```bash
docker exec -it airflow-worker bash -c '
  dbt run  --project-dir /opt/airflow/project/dbt_parallel \
           --profiles-dir /opt/airflow/project/dbt_parallel \
           --select stg_<source_name>__<dataset>+
  dbt test --project-dir /opt/airflow/project/dbt_parallel \
           --profiles-dir /opt/airflow/project/dbt_parallel \
           --select stg_<source_name>__<dataset>+
'
```

#### 6d. Trigger the full DAG

```bash
docker exec -it airflow-worker airflow dags trigger <source_name>_<dataset>_ingestion
```

---

### Step 7 — Verify End-to-End

Check each layer:

```bash
# MinIO bronze — at least one artifact written
docker exec -it airflow-worker python3 -c "
import boto3, os
s3 = boto3.client('s3', endpoint_url=os.environ['MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['MINIO_SECRET_KEY'])
objs = s3.list_objects_v2(Bucket='lakehouse',
    Prefix='bronze/<source_name>/dataset=<dataset>/', MaxKeys=5)
for o in objs.get('Contents', []):
    print(o['Key'], o['Size'])
"

# Postgres silver — records loaded
docker exec -it warehouse psql -U airflow -d freshminds_dw -c \
  "SELECT count(*) FROM <source_name>.<dataset>;"

# dbt marts — fact table populated
docker exec -it warehouse psql -U airflow -d freshminds_dw -c \
  "SELECT count(*) FROM marts.fct_<dataset>;"
```

---

## Checklist for New Sources

Use this checklist to track your progress:

- [ ] `src/ingestion/<source>/config.py` — `SourceTableConfig` defined
- [ ] `src/ingestion/<source>/extract_<dataset>.py` — extractor implemented
- [ ] `src/ingestion/<source>/parse_<dataset>.py` — parser implemented
- [ ] `dbt_parallel/models/staging/<source>/stg_<source>__<dataset>.sql` — staging view
- [ ] `dbt_parallel/models/staging/<source>/_stg_<source>__models.yml` — source + tests
- [ ] `dbt_parallel/models/intermediate/<source>/int_<source>__<dataset>_enriched.sql`
- [ ] `dbt_parallel/models/intermediate/<source>/_int_<source>__models.yml`
- [ ] `dbt_parallel/models/marts/<source>/dim_*.sql` — dimension tables
- [ ] `dbt_parallel/models/marts/<source>/fct_<dataset>.sql` — fact table
- [ ] `dbt_parallel/models/marts/<source>/_marts_<source>__models.yml` — tests + exposures
- [ ] `dags/<source>_<dataset>_ingestion.py` — Airflow DAG
- [ ] DDL verified locally
- [ ] dbt run + test passes (all tests green)
- [ ] End-to-end DAG trigger successful
- [ ] Ingestion state row written to `staging.ingestion_state`

---

## Common Patterns and Tips

### Change Detection via Checksum

The upsert SQL uses a `WHERE checksum IS DISTINCT FROM EXCLUDED.checksum`
guard.  This means:
- If the source data hasn't changed, the row is **not** updated.
- `ingested_at` is preserved (not overwritten on no-change re-runs).
- The `RETURNING` clause only counts rows that were actually modified.

### Multiple Datasets per Source

A single source can have multiple datasets. Create a separate `config.py`
entry, parser, extractor, dbt model set, and DAG for each. They share the
same Postgres schema.

```python
# src/ingestion/acme_portal/config.py
ORDERS_CONFIG = SourceTableConfig(source_name="acme_portal", dataset="orders", ...)
CUSTOMERS_CONFIG = SourceTableConfig(source_name="acme_portal", dataset="customers", ...)
```

### Handling Salesforce / Aura JSON

If the source uses Salesforce Experience Cloud (Lightning/Aura), records are
typically nested in:

```
context.globalValueProviders[type=$Record].values.records
```

**Not** in `actions[].returnValue`.  See `parse_vacatures.py` for the
`_extract_aura_record_provider()` and `_flatten_aura_record()` pattern.

### Idempotent Re-runs

The pipeline is designed for safe re-runs:
- `CREATE TABLE IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` — safe.
- `ON CONFLICT … DO UPDATE` — upsert semantics.
- Checksum guard — prevents unnecessary updates.
- `ingested_at DEFAULT now()` — only set on first insert.

### Adding an __init__.py

Create an empty `__init__.py` in your source directory to make it a Python
package:

```bash
touch src/ingestion/<source_name>/__init__.py
```

---

## Troubleshooting

| Symptom | Likely Cause | Fix |
|---------|-------------|-----|
| `EnvironmentError: Postgres user not set` | Missing `WAREHOUSE_USER` env var or Airflow connection | Check `.env` or add `AIRFLOW_CONN_POSTGRES_WAREHOUSE` |
| `No primary_key column defined` | `SourceTableConfig` has no `Column(…, primary_key=True)` | Add `primary_key=True` to the natural key column |
| dbt `source not found` | Source not registered in `_stg_*__models.yml` | Add a `sources:` block with matching name + schema |
| `0 records parsed` from JSON | Parser not navigating to the correct JSON path | Print the JSON structure; check `_records_from_value()` |
| Duplicate rows in dimension | Dimension key includes a per-row attribute | Move the attribute to the fact table or derive from the key |
| `dbt test` relationship failure | FK column has values not in the dimension | Check for NULLs; coalesce to a default in the intermediate model |
| DAG import error | Module not on `PYTHONPATH` | Verify `PYTHONPATH=/opt/airflow/project` in Dockerfile |
