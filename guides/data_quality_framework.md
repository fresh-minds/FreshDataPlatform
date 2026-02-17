# Centralized Data Quality Framework

This platform now supports a single, centralized quality-rule definition file:

- Rules file: `schema/data_quality_rules.yaml`
- Runner script: `scripts/run_data_quality.py`

The goal is to define checks once and reuse them across domains.

## Why this design

- One rule definition source for all domains and layers.
- Shared check types with consistent severity behavior.
- Reusable command for local runs and orchestration hooks.

## Supported check types

- `columns_exist`
- `row_count_between`
- `non_null`
- `unique`
- `accepted_values`
- `value_range`
- `regex`
- `freshness_hours`
- `expression`
- `foreign_key_exists`

## Commands

```bash
# List configured datasets
make dq-list

# Run one dataset
make dq-check DATASET=finance.fact_grootboekmutaties

# Run all configured datasets
make dq-check-all
```

You can also run directly:

```bash
python scripts/run_data_quality.py --dataset finance.fact_grootboekmutaties
python scripts/run_data_quality.py --all
```

## Rule structure (example)

```yaml
datasets:
  finance.fact_grootboekmutaties:
    domain: finance
    layer: gold
    table: fact_grootboekmutaties
    missing_behavior: warn
    checks:
      - name: transaction_id_not_null
        type: non_null
        columns: [GrootboekmutatieID]
        max_null_pct: 0
        severity: high
```

## Behavior notes

- `missing_behavior` controls dataset-load handling:
  - `fail`: fails checks if dataset cannot be loaded
  - `warn`: records a failed warning-level presence check
  - `skip`: marks dataset as skipped
- Command exit code is driven by check severity and `--fail-on` thresholds.

## Recommended rollout

1. Start with `missing_behavior: warn` for unstable datasets.
2. Stabilize pipelines and column contracts.
3. Move critical datasets to `missing_behavior: fail`.
4. Add the runner as DAG tasks after Silver and Gold writes.
