# Schema Agent Instructions

Scope: `schema/` (DBML, glossary, metrics, data quality rules, standards).

## Must Do
- Keep schema-as-code artifacts synchronized:
  - DBML (`warehouse.dbml`, `domains/*.dbml` when present)
  - Governance metadata (`glossary.yaml`, `metrics.yaml`)
  - Data quality rules (`data_quality_rules.yaml`)
- When schema contracts change, update affected dataset configs in `tests/configs/datasets/`.

## Validation
- Run and report:
  - `make schema-validate`
  - `make governance-validate`
  - `make dq-list`
- Run `make schema-drift-check` when physical schema alignment is part of the change.

## Documentation Requirements
- Update:
  - `schema/README.md`
  - `DATA_MODEL.md` for model/contract-level changes
  - `DEVELOPMENT.md` when workflows/commands change
