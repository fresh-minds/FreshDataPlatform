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

### Run quality checks
```bash
make lint
make test
make qa-test
```

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

## Useful Make Targets
- `make help`: list available targets
- `make run-job-connectors`: run RSS/Sitemap connector runner
- `make schema-validate`: validate DBML conventions
- `make schema-drift-check`: compare warehouse to `schema/warehouse.dbml`
- `make governance-validate`: validate governance metadata completeness
- `make dq-list`, `make dq-check`, `make dq-check-all`: centralized DQ execution

## Code Quality and Standards
- Formatter/linter: Ruff (`make lint`, `make format`)
- Type checking: MyPy (`make type-check`)
- Testing: Pytest suites under `tests/`
- Packaging: `pyproject.toml` + editable install (`pip install -e ".[dev]"`)

## Adding a New Pipeline
1. Implement ingestion/transform logic in `pipelines/<domain>/`.
2. Register callable in `scripts/run_local.py` if it should be runnable from the generic local runner.
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
  - `python scripts/sync_dbml_to_datahub.py`
  - `python scripts/register_datahub_catalog.py`

## Troubleshooting
- Services not healthy:
  - `docker compose ps`
  - `docker compose logs --tail=200`
- E2E failures:
  - inspect `tests/e2e/evidence/latest/`
- SSO failures:
  - inspect `tests/sso/artifacts/latest/`
  - verify Keycloak host mapping (`127.0.0.1 keycloak`)
