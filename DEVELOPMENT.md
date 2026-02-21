# Development Guide

## Local Prerequisites
- Python `3.9+`
- Docker + Docker Compose
- Make
- Node.js `18+` (frontend only)

Optional for Kubernetes paths:
- `kubectl`, `kind`, `az`, `docker buildx`

## Initial Setup
```bash
cp .env.template .env
python3 -m venv .venv
source .venv/bin/activate
make dev-install
```

## Daily Workflow
### Start core services
```bash
docker compose up -d
```

### Run pipeline flows
```bash
make run-job-market
LOCAL_MOCK_PIPELINES=false make run PIPELINE=job_market_nl.bronze_cbs_vacancy_rate
```

### Run Source SP1 portal DAG (Airflow)
Prerequisite: set `SP1_USERNAME` and
`SP1_PASSWORD` in `.env`, then refresh Airflow containers.

```bash
docker compose up -d airflow-webserver airflow-scheduler
docker exec airflow-webserver airflow dags trigger source_sp1_vacatures_ingestion
docker exec airflow-webserver airflow dags list-runs -d source_sp1_vacatures_ingestion --no-backfill
```

### Run quality checks
```bash
make lint
make test
make qa-test
make observability-verify
```

Governance suite note:
- `tests/governance/test_governance_controls.py` bootstraps `platform_audit.pipeline_runs`
  with a deterministic seed row when the table is absent, so local and CI runs are
  stable without requiring a prior E2E pipeline execution.

### Run full E2E suites
```bash
make test-e2e
make test-sso
```

## Frontend Development
Run the launchpad locally:

```bash
cd frontend
npm install
npm run dev
```

Default dev URL: `http://localhost:3000`

Service links are resolved via:
- Vite env vars (`VITE_*_URL`)
- Fallback localhost endpoints in `frontend/src/config/serviceUrls.js`

Platform dashboard note:
- `/platform` includes a dedicated "Logging, monitoring and tracing" section with links to Grafana, Prometheus, and Alertmanager.
- Optional frontend overrides: `VITE_GRAFANA_URL`, `VITE_PROMETHEUS_URL`, `VITE_ALERTMANAGER_URL`.
- `/architecture` and `/services` expose the same observability links for consistent navigation.
- In `/architecture`, the observability nodes in the SVG diagram (Grafana, Prometheus, Alertmanager) are also directly clickable.

## Useful Make Targets
- `make help`: list available targets
- `make run-job-connectors`: run RSS/Sitemap connector runner
- `make observability-verify`: validate Compose logs/metrics/traces ingestion path (Grafana/Loki/Prometheus/Tempo); supports strict trace-volume mode (`OBS_REQUIRE_TRACE_VOLUME=true`) and ambient-only mode (`OBS_TRACE_VOLUME_MODE=ambient`)
- `make schema-validate`: validate DBML conventions
- `make schema-drift-check`: compare warehouse to `schema/warehouse.dbml`
- `make governance-validate`: validate governance metadata completeness
- `make dq-list`, `make dq-check`, `make dq-check-all`: centralized DQ execution

## Code Quality and Standards
- Formatter/linter: Ruff (`make lint`, `make format`)
- Type checking: MyPy (`make type-check`)
- Testing: Pytest suites under `tests/`
- Packaging: `pyproject.toml` + editable install (`pip install -e ".[dev]"`)

## Adding a New Ingestion Source

The ingestion framework under `src/ingestion/` provides templates and generic
helpers for onboarding new data sources into the medallion pipeline
(Bronze → Silver → Gold).

Quick steps:
1. Copy `src/ingestion/_template/` to `src/ingestion/<source_name>/`.
2. Define a `SourceTableConfig` in `config.py`.
3. Write an extractor and parser.
4. Copy dbt model templates from `dbt_parallel/_model_templates/`.
5. Copy `dags/_template_dag.py` and wire everything together.
6. Verify locally: `dbt run + test`, trigger DAG.

Full walkthrough: [Data Ingestion Guide](docs/INGESTION_GUIDE.md)

## Adding a New Pipeline (Spark/Python)
1. Implement ingestion/transform logic in `pipelines/<domain>/`.
2. Register callable in `scripts/pipeline/run_local.py` if it should be runnable from the generic local runner.
3. Add/extend DAG wiring in `dags/` if orchestration is needed.
4. Define validation rules in:
   - `schema/data_quality_rules.yaml`
   - `tests/configs/datasets/*.yml` (if contract/governance checks apply)
5. Add tests in `tests/unit`, `tests/integration`, or E2E suites.

## Adding a New Job Connector
1. Implement connector class in `shared/job_connectors/connectors/`.
2. Register it in `shared/job_connectors/registry.py`.
3. Add deterministic fixtures and parser tests under:
   - `tests/fixtures/job_connectors/<connector>/`
   - `tests/unit/test_job_connectors_*.py`
4. Enforce allowlist/robots behavior via `JOB_CONNECTORS_*` env config.

## Governance and Metadata Development
- Update glossary/metrics/rules in `schema/`.
- Validate with:
  - `make schema-validate`
  - `make governance-validate`
- Publish metadata with:
  - `python scripts/catalog/sync_dbml_to_datahub.py`
  - `python scripts/catalog/register_datahub_catalog.py`

## Troubleshooting
- Services not healthy:
  - `docker compose ps`
  - `docker compose logs --tail=200`
- E2E failures:
  - inspect `tests/e2e/evidence/latest/`
- SSO failures:
  - inspect `tests/sso/artifacts/latest/`
  - verify `KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL` points to a browser-reachable host (default: `http://localhost:8090/.../auth`)
