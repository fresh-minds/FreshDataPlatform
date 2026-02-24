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

Required before `docker compose up -d`:
- Set `MINIO_SSO_BRIDGE_SESSION_SECRET` in `.env` to a non-placeholder value (32+ chars).

Generate one locally:

```bash
python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
```

Verification:

```bash
grep '^MINIO_SSO_BRIDGE_SESSION_SECRET=' .env
```

Superset OAuth security defaults:
- `SUPERSET_OAUTH_DEFAULT_ROLE=Gamma` (least privilege)
- `SUPERSET_WTF_CSRF_ENABLED=true`
- If you intentionally need OAuth auto-admin for a controlled demo, set both:
  - `SUPERSET_OAUTH_DEFAULT_ROLE=Admin`
  - `SUPERSET_ALLOW_AUTO_ADMIN_ROLE=true`

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

Homepage assistant note:
- On `/`, a diagonal `Demo` ribbon is pinned to the top-left corner of the page.
- Toggle the ribbon with `VITE_SHOW_DEMO_RIBBON=true|false` (default `true`).
- Enable demo SSO bootstrap with `VITE_DEMO_AUTO_ADMIN=true` (forces real Keycloak login flow and uses demo username hint for SSO).
- Set the demo username hint with `VITE_DEMO_USERNAME` (default `odp-admin`).
- `KEYCLOAK_DEMO_AUTO_LOGIN` is disabled by default; only set it to `true` for isolated local demos where auto-submitting demo credentials is acceptable.
- When enabled, set `KEYCLOAK_DEMO_AUTOLOGIN_USERNAME` (default `odp-admin`) and keep Keycloak on a local hostname.
- On `/`, clicking the hero image opens a chat panel on the right.
- The panel calls `portal-api` endpoint `POST /api/chat` (authenticated with the Keycloak bearer token).
- Preferred backend env vars (Azure AI Foundry Agent): `AZURE_EXISTING_AIPROJECT_ENDPOINT` and either `AZURE_EXISTING_AGENT_ID` or `AZURE_EXISTING_AGENT_NAME`.
- Backward compatibility: `AZURE_FOUNDRY_AGENT_ENDPOINT` / `AZURE_FOUNDRY_AGENT_ID` / `AZURE_FOUNDRY_AGENT_NAME` are still accepted as legacy aliases.
- Foundry auth for `portal-api` supports two modes:
  - API key mode: set `AZURE_FOUNDRY_API_KEY`.
  - DefaultAzureCredential mode: leave `AZURE_FOUNDRY_API_KEY` empty and provide `AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET` (or another valid DefaultAzureCredential source).
- In containerized local runs, Azure CLI login on host is not used automatically by `portal-api`; prefer service principal env vars for DefaultAzureCredential mode.
- If chat returns a Foundry `403 Forbidden`, assign the calling identity permission for `Microsoft.MachineLearningServices/workspaces/agents/action` on the target AI Foundry project/workspace.
- OpenAI fallback is removed; homepage chat uses Foundry agent only.

Platform dashboard note:
- `/platform` keeps "Overview" at the top, followed by ordered destinations (Orchestration, Storage, Analytics + Notebook workspace row, Catalog & lineage).
- The "People" section is shown in a separate box, only visible to admins, and rendered at the very bottom.
- `/platform` includes a dedicated "Logging, monitoring and tracing" section with links to Grafana, Prometheus, and Alertmanager.
- `/platform` includes a dedicated "docs and horizontal technical lineage" section with a `dbt docs & lineage` link.
- Optional frontend overrides: `VITE_GRAFANA_URL`, `VITE_PROMETHEUS_URL`, `VITE_ALERTMANAGER_URL`.
- Optional frontend override for dbt docs: `VITE_DBT_DOCS_URL`.
- `/architecture` and `/services` expose the same observability links for consistent navigation.
- In `/architecture`, the observability nodes in the SVG diagram (Grafana, Prometheus, Alertmanager) are also directly clickable.
- The `/architecture` diagram also reflects active runtime components from Compose, including Jupyter (`:8888`) and tracing/log backends (Loki `:3100`, Tempo `:3200`).

dbt docs + lineage workflow:
- Generate docs artifacts: `make dbt-docs-generate`
- Generate and (re)start static docs host: `make dbt-docs-refresh`
- Keep docs auto-updated while developing dbt logic: `make dbt-docs-watch`
- Open docs UI directly at `http://localhost:8089` or via `/platform` -> "docs and horizontal technical lineage".

## Useful Make Targets
- `make help`: list available targets
- `make run-job-connectors`: run RSS/Sitemap connector runner
- `make observability-verify`: validate Compose logs/metrics/traces ingestion path (Grafana/Loki/Prometheus/Tempo); supports strict trace-volume mode (`OBS_REQUIRE_TRACE_VOLUME=true`) and ambient-only mode (`OBS_TRACE_VOLUME_MODE=ambient`)
- `make k8s-aks-smoke`: run in-cluster AKS smoke checks (observability + core service endpoints); HTTP checks retry for short warm-up windows (~60s max per endpoint) and then fail on RED checks
- `make k8s-aks-up`: runs AKS smoke checks by default after deploy (`AKS_SMOKE_AFTER_UP` unset/empty = `true`) and uses Azure Key Vault as the default AKS secret source; reruns safely skip Key Vault provider re-enable when already active and can enforce minimum System nodepool capacity via `AKS_NODE_COUNT` (set `AKS_SMOKE_AFTER_UP=false` to skip smoke; set `AKS_USE_KEY_VAULT=false` to use direct `.env` -> Kubernetes secret)
- `make k8s-aks-update-images`: build/push selected app images and patch existing AKS deployments only (faster inner loop; no infra/parity apply)
- `make dbt-docs-generate`: generate dbt docs site artifacts in `dbt_parallel/target/`
- `make dbt-docs-refresh`: regenerate dbt docs and ensure static docs service is running
- `make dbt-docs-watch`: auto-regenerate dbt docs whenever files in `dbt_parallel/models|macros|snapshots|seeds|tests` change
- `make schema-validate`: validate DBML conventions
- `make schema-drift-check`: compare warehouse to `schema/warehouse.dbml`
- `make governance-validate`: validate governance metadata completeness
- `make dq-list`, `make dq-check`, `make dq-check-all`: centralized DQ execution

## Code Quality and Standards
- Formatter/linter: Ruff (`make lint`, `make format`)
- Type checking: MyPy (`make type-check`)
- Testing: Pytest suites under `tests/`
- Packaging: `pyproject.toml` + editable install (`pip install -e ".[dev]"`)
- Third-party runtime license triage: `make license-risk-check`

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
