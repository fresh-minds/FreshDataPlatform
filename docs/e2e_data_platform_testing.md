# E2E Data & Analytics Platform Testing

This repository now includes a config-driven QA framework that validates medallion flows, contracts, orchestration behavior, and governance controls end-to-end.

## Platform Mapping (Detected)

- Orchestrator: Airflow DAGs in `dags/`.
- Transformation engine: dbt project in `dbt_parallel/` with source tables in `job_market_nl` and serving models in `job_market_nl_dbt`.
- Warehouse/Serving: PostgreSQL warehouse (`job_market_nl` schema), plus Superset SQL templates.
- Contracts: dbt model YAML + config-driven dataset contracts in `tests/configs/datasets/*.yml`.
- Governance metadata: `schema/metrics.yaml`, governance validation script, and QA governance policies in `tests/configs/policies/governance_policies.yml`.

## QA Structure

- `tests/data_quality/`: baseline schema/null/unique/range/freshness checks.
- `tests/contracts/`: schema contract and naming/PK stability checks.
- `tests/e2e/`: pipeline execution, idempotency, incremental behavior, serving query checks.
- `tests/governance/`: metadata completeness, lineage, PII, RBAC, retention, auditability.
- `tests/helpers/`: connectors, policy engine, shared SQL assertions, environment config.
- `tests/configs/datasets/`: dataset-level contracts + expectations.
- `tests/configs/policies/`: governance policy rules.
- `tests/configs/environments.yml`: dev/test/prod isolation and mutation safety flags.

## Run Locally

1. Start platform services (warehouse + dependencies) and install deps:

```bash
make dev-install
docker compose up -d
```

2. Run full QA E2E suite with artifacts:

```bash
make test-e2e
```

3. Evidence and reports are written to:

- `tests/e2e/evidence/latest/results/report.html`
- `tests/e2e/evidence/latest/results/qa_report.md`
- `tests/e2e/evidence/latest/results/qa_report.json`
- `tests/e2e/evidence/latest/results/junit.xml`

## Run in CI

The GitHub workflow `.github/workflows/e2e-data-platform.yml` runs on pull requests affecting pipeline/test/governance assets and executes:

```bash
./scripts/testing/run_e2e_tests.sh
```

It uploads `tests/e2e/evidence/latest` as the evidence artifact.

## Add a New Dataset Test

1. Add a dataset contract file in `tests/configs/datasets/<dataset>.yml`.

Minimum required fields:

```yaml
dataset: schema.table
owner: team@example.com
description: "What this dataset is for"
domain: job_market_nl
layer: gold
classification: confidential
sensitivity: internal
product_tag: labor-market
pii_columns: []
pii_classifications: {}
retention_days: 365
timestamp_column: loaded_at
primary_key: [id]
upstreams: [source_schema.source_table]
tests:
  freshness:
    column: loaded_at
    format: timestamp
    max_age_hours: 24
  schema:
    required_columns: [id, loaded_at]
    column_types:
      id: text
  constraints:
    unique: [id]
    not_null: [id]
  reconciliations:
    - type: row_count_ratio
      upstream: source_schema.source_table
      min_ratio: 0.95
      max_ratio: 1.05
governance:
  require_lineage: true
  require_classification: true
  require_rbac: false
  pii_masking_required: false
  allowed_roles_read: []
```

2. If policy behavior needs to change, update `tests/configs/policies/governance_policies.yml`.

3. Run:

```bash
make qa-test
```

4. If needed, add a focused test in one of:

- `tests/data_quality/`
- `tests/contracts/`
- `tests/e2e/`
- `tests/governance/`
