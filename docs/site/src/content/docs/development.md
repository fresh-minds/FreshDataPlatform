---
title: Development
description: Local setup, workflows, Make targets, and troubleshooting.
order: 4
---

## Local Prerequisites

- Python `3.9+`
- Docker + Docker Compose
- Make
- Node.js `18+` (frontend only)

Optional for Kubernetes paths: `kubectl`, `kind`, `az`, `docker buildx`

## Initial Setup

```bash
cp .env.template .env
python3 -m venv .venv
source .venv/bin/activate
make dev-install
```

Required before `docker compose up -d`:
- Set `MINIO_SSO_BRIDGE_SESSION_SECRET` in `.env` to a non-placeholder value (32+ chars)

## Daily Workflow

### Start core services
```bash
docker compose up -d
```

### Start bare minimum services
```bash
make compose-up-minimal
```

### Run pipeline flows
```bash
make run-odp-staffing-demand
make run-odp-staffing-demand-metadata
```

### Run quality checks
```bash
make lint
make test
make qa-test
make observability-verify
```

### Run full E2E suites
```bash
make test-e2e
make test-sso
```

## Frontend Development

```bash
cd frontend
npm install
npm run dev
```

Default dev URL: `http://localhost:3000`

Service links are resolved via Vite env vars (`VITE_*_URL`) with fallback localhost endpoints in `frontend/src/config/serviceUrls.js`.

## Adding a New Data Source

Quick steps:
1. Copy `src/ingestion/_template/` to `src/ingestion/<source_name>/`
2. Define a `SourceTableConfig` in `config.py`
3. Write an extractor and parser
4. Copy dbt model templates from `dbt/_model_templates/bronze|silver|gold`
5. Copy `dags/_template_dag.py` and wire everything together
6. Verify locally: `dbt run + test`, trigger DAG

Full walkthrough: [Data Ingestion Guide](/FreshDataPlatform/docs/ingestion-guide/)

## Useful Make Targets

| Target | Description |
|--------|-------------|
| `make help` | List available targets |
| `make dev-install` | Install development dependencies |
| `make compose-up-minimal` | Start minimal Docker stack |
| `make run-odp-staffing-demand` | Run end-to-end pipeline |
| `make lint` | Run linter |
| `make test` | Run test suite |
| `make qa-test` | Run governance/quality tests |
| `make test-e2e` | Full E2E test with evidence |
| `make observability-verify` | Validate observability ingestion |
| `make dbt-docs-refresh` | Generate and host dbt docs |
| `make schema-validate` | Validate DBML conventions |
| `make governance-validate` | Validate governance metadata |

## Code Quality and Standards

- **Formatter/linter**: Ruff (`make lint`, `make format`)
- **Type checking**: MyPy (`make type-check`)
- **Testing**: Pytest suites under `tests/`
- **Packaging**: `pyproject.toml` + editable install (`pip install -e ".[dev]"`)

## Troubleshooting

- **Services not healthy**: `docker compose ps` and `docker compose logs --tail=200`
- **Pipeline fails on dbt**: Ensure Airflow services are running and `dbt` is available in the worker runtime
- **E2E failures**: Inspect `tests/e2e/evidence/latest/`
- **SSO failures**: Inspect `tests/sso/artifacts/latest/` and verify `KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL`
