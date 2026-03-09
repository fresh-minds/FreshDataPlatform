---
title: Testing
description: E2E testing strategy, dataset contracts, and QA framework.
order: 8
---

## Overview

The repository includes a config-driven QA framework that validates medallion flows, contracts, orchestration behavior, and governance controls end-to-end.

## Platform Mapping

- **Orchestrator**: Airflow DAGs in `dags/`
- **Transformation engine**: dbt project in `dbt/`
- **Warehouse/Serving**: PostgreSQL warehouse with Superset SQL templates
- **Contracts**: dbt model YAML + config-driven dataset contracts in `tests/configs/datasets/*.yml`
- **Governance metadata**: `schema/metrics.yaml`, governance validation, QA policies

## QA Structure

| Directory | Purpose |
|-----------|---------|
| `tests/data_quality/` | Baseline schema/null/unique/range/freshness checks |
| `tests/contracts/` | Schema contract and naming/PK stability checks |
| `tests/e2e/` | Pipeline execution, idempotency, incremental behavior, serving queries |
| `tests/governance/` | Metadata completeness, lineage, PII, RBAC, retention, auditability |
| `tests/helpers/` | Connectors, policy engine, shared SQL assertions, environment config |
| `tests/configs/datasets/` | Dataset-level contracts + expectations |
| `tests/configs/policies/` | Governance policy rules |
| `tests/configs/environments.yml` | Dev/test/prod isolation and mutation safety flags |

## Run Locally

1. Start platform services and install deps:
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

The GitHub workflow `.github/workflows/e2e-data-platform.yml` runs on pull requests and executes `./scripts/testing/run_e2e_tests.sh`, uploading evidence as an artifact.

## Add a New Dataset Test

1. Add a dataset contract file in `tests/configs/datasets/<dataset>.yml`:

```yaml
dataset: schema.table
owner: team@example.com
description: "What this dataset is for"
domain: odp_staffing_demand
layer: gold
classification: confidential
sensitivity: internal
product_tag: labor-market
pii_columns: []
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
governance:
  require_lineage: true
  require_classification: true
```

2. Update policies if needed in `tests/configs/policies/governance_policies.yml`

3. Run:
```bash
make qa-test
```
