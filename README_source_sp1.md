# Source SP1 portal — Vacatures Ingestion Runbook

End-to-end runbook for the `source_sp1_vacatures_ingestion` Airflow DAG.

---

## Table of Contents

1. [Architecture overview](#1-architecture-overview)
2. [Prerequisites](#2-prerequisites)
3. [Configuration — Airflow Connections](#3-configuration--airflow-connections)
4. [Environment variable fallbacks](#4-environment-variable-fallbacks)
5. [Running locally](#5-running-locally)
6. [What fields are captured](#6-what-fields-are-captured)
7. [How incremental ingestion works](#7-how-incremental-ingestion-works)
8. [Extraction method selection](#8-extraction-method-selection)
9. [MFA fallback](#9-mfa-fallback)
10. [Known failure modes](#10-known-failure-modes)
11. [dbt model summary](#11-dbt-model-summary)
12. [Monitoring and observability](#12-monitoring-and-observability)

---

## 1. Architecture overview

```
Source SP1 portal (Salesforce Experience Cloud)
        │
        │  Playwright (headless Chromium)
        ▼
[Bronze: MinIO]
  lakehouse/
    bronze/source_sp1/
      dataset=vacatures/
        run_dt=YYYY-MM-DD/
          raw/{artifact_id}.{json|csv|xlsx|html}
          meta/{artifact_id}.json          ← provenance metadata
        dataset=dbt_artifacts/
          run_dt=YYYY-MM-DD/
            raw/dbt_manifest_*.json
            raw/dbt_run_results_*.json
        │
        │  Python parser
        ▼
[Silver: Postgres]
  source_sp1.vacatures           ← upserted by pipeline
  staging.ingestion_state                  ← incremental cursor
        │
        │  dbt
        ▼
[Gold: dbt view]
  <dbt_schema>.stg_source_sp1__vacatures
```

The portal (`<portal-url>`) is built on
**Salesforce Experience Cloud**.  The extraction uses Playwright to
authenticate and then chooses the most stable available method (export
download > XHR JSON capture > DOM HTML).

---

## 2. Prerequisites

```bash
# Python packages (already in pyproject.toml)
pip install playwright beautifulsoup4 boto3 psycopg2-binary

# Install Playwright browser binaries (run once per environment)
python -m playwright install chromium
python -m playwright install-deps    # Linux only — installs OS dependencies
```

For Airflow Docker Compose runs, the Airflow image now bundles Playwright,
the Chromium browser, and the `dbt-postgres` CLI. If you are upgrading an
existing stack, rebuild the Airflow image:

```bash
docker compose build airflow-webserver airflow-scheduler
```

---

## 3. Configuration — Airflow Connections

### `source_sp1`

| Field     | Value                                                     |
|-----------|-----------------------------------------------------------|
| Conn Type | `HTTP`                                                    |
| Login     | Portal email / username                                   |
| Password  | Portal password                                           |
| Extra     | `{"base_url": "<portal-url>"}`      |

**Airflow UI:**
1. Admin → Connections → ＋
2. Connection Id: `source_sp1`
3. Connection Type: `HTTP`
4. Login: `your.email@example.com`
5. Password: `••••••••`
6. Extra (JSON): `{"base_url": "<portal-url>"}`

**CLI:**
```bash
airflow connections add source_sp1 \
  --conn-type http \
  --conn-login your.email@example.com \
  --conn-password 'your-password' \
  --conn-extra '{"base_url": "<portal-url>"}'
```

---

### `minio`

| Field     | Value                          |
|-----------|--------------------------------|
| Conn Type | `HTTP`                         |
| Host      | `http://minio:9000`            |
| Login     | MinIO access key               |
| Password  | MinIO secret key               |

**CLI:**
```bash
airflow connections add minio \
  --conn-type http \
  --conn-host http://minio:9000 \
  --conn-login minioadmin \
  --conn-password minioadmin
```

---

### `postgres_warehouse`

| Field     | Value                          |
|-----------|--------------------------------|
| Conn Type | `Postgres`                     |
| Host      | `warehouse`                    |
| Schema    | `open_data_platform_dw`        |
| Login     | Warehouse user                 |
| Password  | Warehouse password             |
| Port      | `5432`                         |

**CLI:**
```bash
airflow connections add postgres_warehouse \
  --conn-type postgres \
  --conn-host warehouse \
  --conn-schema open_data_platform_dw \
  --conn-login admin \
  --conn-password admin \
  --conn-port 5432
```

---

## 4. Environment variable fallbacks

If Airflow Connections are not set, the pipeline reads these env vars:
In the Docker Compose stack, these values are passed into `airflow-scheduler`
(and `airflow-webserver` for ad-hoc task debugging), so setting them in `.env`
is enough for local DAG runs.

| Variable                            | Default                              |
|-------------------------------------|--------------------------------------|
| `SP1_USERNAME`     | *(required)*                         |
| `SP1_PASSWORD`     | *(required)*                         |
| `SP1_BASE_URL`     | `<portal-url>` |
| `MINIO_ENDPOINT`                    | `http://minio:9000`                  |
| `MINIO_ACCESS_KEY`                  | *(required)*                         |
| `MINIO_SECRET_KEY`                  | *(required)*                         |
| `MINIO_BUCKET`                      | `lakehouse`                          |
| `WAREHOUSE_HOST`                    | `warehouse`                          |
| `WAREHOUSE_PORT`                    | `5432`                               |
| `WAREHOUSE_DB`                      | `open_data_platform_dw`              |
| `WAREHOUSE_USER`                    | *(required)*                         |
| `WAREHOUSE_PASSWORD`                | *(required)*                         |
| `INCREMENTAL_LOOKBACK_DAYS`         | `7`                                  |
| `RATE_LIMIT_RPS_PER_DOMAIN`         | `0.2`  (one request per 5 s)         |
| `MAX_VACATURE_PAGES`                | `50`                                 |

---

## 5. Running locally

### Option A — trigger via Airflow UI

```bash
# Ensure SP1 credentials exist in .env (required for full extract)
# SP1_USERNAME=you@example.com
# SP1_PASSWORD=yourpassword
# SP1_BASE_URL=<portal-url>

# Start or refresh Airflow services after editing .env
docker compose up -d
docker compose up -d airflow-webserver airflow-scheduler

# Open http://localhost:8080, login, find the DAG, and trigger it.
```

### Option A verification — run status + data visibility

```bash
# Load .env values in your current shell
set -a
source .env
set +a

# 1) DAG run state (must be "success" for a fully successful pipeline run)
docker exec airflow-webserver airflow dags list-runs \
  -d source_sp1_vacatures_ingestion --no-backfill

# 2) MinIO bronze artifacts for this source
python - <<'PY'
import os
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
    aws_access_key_id=os.getenv("MINIO_ROOT_USER") or os.getenv("MINIO_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD") or os.getenv("MINIO_SECRET_KEY"),
)
prefix = "bronze/source_sp1/dataset=vacatures/"
resp = s3.list_objects_v2(Bucket="lakehouse", Prefix=prefix, MaxKeys=20)
print("objects:", resp.get("KeyCount", 0))
for obj in resp.get("Contents", []):
    print(obj["Key"])
PY

# 3) Warehouse row counts
docker exec open-data-platform-warehouse psql \
  -U "${WAREHOUSE_USER:-admin}" \
  -d "${WAREHOUSE_DB:-open_data_platform_dw}" \
  -c "select count(*) as vacatures_count from source_sp1.vacatures;" \
  -c "select count(*) as stg_count from staging.stg_source_sp1__vacatures;"
```

Superset check (UI):
1. Open `http://localhost:8088`.
2. In SQL Lab, run:
   - `select * from source_sp1.vacatures order by ingested_at desc limit 50;`
   - `select * from staging.stg_source_sp1__vacatures order by ingested_at desc limit 50;`

> DAG state note: this DAG now includes a `pipeline_success_gate` leaf task, so
> preflight/extract/load/dbt failures correctly mark the DAG run as failed.

### Option B — run extraction standalone (debugging)

```bash
# From the project root:
export SP1_USERNAME="you@example.com"
export SP1_PASSWORD="yourpassword"
export MINIO_ENDPOINT="http://localhost:9000"
export MINIO_ACCESS_KEY="minioadmin"
export MINIO_SECRET_KEY="minioadmin"
export WAREHOUSE_HOST="localhost"
export WAREHOUSE_PORT="5433"
export WAREHOUSE_DB="open_data_platform_dw"
export WAREHOUSE_USER="admin"
export WAREHOUSE_PASSWORD="admin"

python - <<'EOF'
from playwright.sync_api import sync_playwright
from src.ingestion.source_sp1.auth import get_credentials, login
from src.ingestion.source_sp1.extract_vacatures import extract_all

creds = get_credentials()
with sync_playwright() as pw:
    browser = pw.chromium.launch(headless=False)   # headless=False for visual debugging
    page = browser.new_page()
    login(page, creds)
    artifacts = extract_all(page=page, base_url=creds.base_url, run_id="test", run_dt="2024-01-01")
    print(f"Extracted {len(artifacts)} artifacts")
    for a in artifacts:
        print(f"  {a.artifact_id}: {a.extraction_method}, {len(a.raw_bytes)} bytes")
    browser.close()
EOF
```

### Option C — run dbt only

```bash
cd dbt_parallel
dbt deps
dbt run --select stg_source_sp1__vacatures+
dbt test --select stg_source_sp1__vacatures
```

If you run dbt outside the project directory (or from Airflow), pass the
paths as subcommand options:

```bash
dbt run --project-dir /opt/airflow/dbt_parallel --profiles-dir /opt/airflow/dbt_parallel \
  --select stg_source_sp1__vacatures+
```

---

## 6. What fields are captured

| Column               | Type      | Source                                            |
|----------------------|-----------|---------------------------------------------------|
| `vacature_id`        | text PK   | Salesforce record ID or deterministic hash        |
| `title`              | text      | Function title / job title                        |
| `status`             | text      | Portal status (Open, Gesloten, Inactief, …)       |
| `client_name`        | text      | Client / account name                             |
| `location`           | text      | Work location (city / region)                     |
| `publish_date`       | date      | Publication date                                  |
| `closing_date`       | date      | Expiration date                                   |
| `hours`              | numeric   | Hours/week (midpoint of range if range given)     |
| `hours_text`         | text      | Raw hours field ("32-40 uur")                     |
| `description`        | text      | Full vacancy description                          |
| `category`           | text      | Discipline / sector / function group              |
| `updated_at_source`  | timestamp | Last-modified date from Salesforce                |
| `source_url`         | text      | Portal page URL                                   |
| `bronze_object_path` | text      | MinIO S3 key of the raw artifact                  |
| `checksum_sha256`    | text      | Content checksum for change detection             |
| `ingested_at`        | timestamp | Pipeline write timestamp (auto-updated)           |
| `is_active`          | boolean   | Derived: true when not closed and not expired     |

---

## 7. How incremental ingestion works

1. On **first run**: all visible vacatures are fetched and inserted.
2. On **subsequent runs**:
   - The `staging.ingestion_state` table records `last_success_utc` and
     `cursor_json` (contains `run_id` and `run_dt`).
   - The extractor re-fetches all visible vacatures (portal does not expose a
     reliable "changed since" filter in the standard UI).
   - The upsert uses `ON CONFLICT (vacature_id) DO UPDATE … WHERE checksum
     IS DISTINCT FROM EXCLUDED.checksum` — rows that have not changed are
     **not written**, making repeated runs fully idempotent.
   - `INCREMENTAL_LOOKBACK_DAYS` (default 7) controls how far back the
     pipeline looks when a filter is available; it has no effect on the
     full-listing fallback.

**Zero-extraction guard:** if `extracted_count = 0` and the previous run was
successful with `extracted_count > 0`, the observability task logs a warning.
Wire this to an alerting webhook (see `scripts/platform/send_alert.py`) for
automated notification.

---

## 8. Extraction method selection

The pipeline tries three strategies in order:

| Priority | Method            | When used                                              |
|----------|-------------------|--------------------------------------------------------|
| 1        | `export_download` | Portal has a visible Export / Download button          |
| 2        | `xhr_json`        | Salesforce Aura / REST API JSON captured from browser  |
| 3        | `dom_html`        | Last resort — HTML table / card parsing via BS4        |

The `extraction_method` field in the bronze meta JSON records which strategy
was used for each artifact.

---

## 9. MFA fallback

If the Source SP1 portal enforces **multi-factor authentication**
(Salesforce Authenticator, TOTP, or SMS), the automated flow will stall after
credential submission.

**Workaround:**

1. Run the extraction in **non-headless** mode with Playwright's storage state:
   ```python
   # scripts/generate_portal_auth_state.py  (one-off manual step)
   from playwright.sync_api import sync_playwright, StorageState
   with sync_playwright() as pw:
       browser = pw.chromium.launch(headless=False)
       context = browser.new_context()
       page = context.new_page()
       page.goto("<portal-url>")
       # Complete MFA manually in the browser window that opens
       input("Complete MFA in browser, then press Enter…")
       context.storage_state(path="portal_auth_state.json")
       browser.close()
   ```

2. Store `portal_auth_state.json` securely (Airflow Variable or secret).
3. In `auth.py`, load the storage state:
   ```python
   context = browser.new_context(storage_state="portal_auth_state.json")
   ```
4. Session cookies typically last hours to days; re-run the one-off step
   when they expire.

> **Note:** never commit `portal_auth_state.json` to version control.

---

## 10. Known failure modes

| Symptom                                | Likely cause                          | Fix                                              |
|----------------------------------------|---------------------------------------|--------------------------------------------------|
| `Authentication failed`                | Wrong credentials                     | Update `AIRFLOW_CONN_SOURCE_SP1`        |
| Login stalls after credentials         | MFA enforced                          | See §9 MFA fallback                              |
| `HTTP 403` in XHR capture              | Session expired or insufficient perms | Re-run; check account permissions               |
| `HTTP 429` in XHR capture             | Rate limited by portal                | Increase `RATE_LIMIT_RPS_PER_DOMAIN` delay       |
| `extracted_count = 0`                  | Portal empty, selectors changed       | Run headless=False; inspect DOM; update selectors|
| Postgres `connection refused`          | Wrong WAREHOUSE_HOST / PORT           | Check env vars; confirm warehouse container up   |
| MinIO `NoSuchBucket`                   | Bucket not created                    | Preflight task creates it; re-run preflight      |
| `dbt executable not found on PATH`     | dbt CLI missing in Airflow image      | Rebuild Airflow image with `dbt-postgres`        |
| dbt `--project-dir` path missing       | Airflow image stores code under `/opt/airflow/project` | Use `/opt/airflow/project/dbt_parallel`          |
| dbt `relation does not exist`          | Source table not yet created          | Run parse_load task first; check DDL             |
| `vacature_id` all `hash_*` values      | Portal gives no stable ID             | Expected for some portals; hash is deterministic |

---

## 11. dbt model summary

### Source (Python-loaded)

| Table                              | Schema                   |
|------------------------------------|--------------------------|
| `source_sp1.vacatures`   | Postgres source schema   |

### dbt staging model

| Model                                       | Materialization | Schema  |
|---------------------------------------------|-----------------|---------|
| `stg_source_sp1__vacatures`       | view            | staging |

**Tests applied:**
- `vacature_id`: `not_null`, `unique`
- `checksum_sha256`: `not_null`
- `ingested_at`: `not_null`
- `status`: `not_null` (severity: warn — portal may not expose status)
- Source freshness: warn after 48 h, error after 168 h

**Selector for partial runs:**
```bash
dbt run  --select stg_source_sp1__vacatures+
dbt test --select stg_source_sp1__vacatures
```

---

## 12. Monitoring and observability

StatsD metrics emitted (to `statsd-exporter:9125`):

| Metric                                              | Description           |
|-----------------------------------------------------|-----------------------|
| `airflow.source_sp1_vacatures.extracted_count` | Bronze artifacts written |
| `airflow.source_sp1_vacatures.parsed_count`    | Records parsed        |
| `airflow.source_sp1_vacatures.upserted_count`  | Postgres rows affected |

Grafana dashboard: add a panel pointing to the `airflow.*` metrics namespace.

Alerting: wire the `on_failure_callback` to `scripts/platform/send_alert.py`
(MS Teams webhook) by setting `ALERT_TEAMS_WEBHOOK_URL` in the environment.
