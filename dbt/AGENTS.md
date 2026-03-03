# dbt Agent Instructions

Scope: `dbt/` (dbt models, snapshots, macros, project config).

## Must Do
- Keep model layering consistent:
  - `models/bronze/` -> `brz_*`
  - `models/silver/` -> `slv_*`
  - `models/gold/` -> `dim_*`, `fact_*`
- Update YAML definitions when model/source names or columns change.
- Run and report relevant checks after changes:
  - `.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt`
  - `.venv/bin/dbt run --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'`
  - `.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'`

## Documentation Requirements
- If transformations, lineage mapping, or run steps change, update:
  - `dbt/README.md`
  - `dbt/TRANSFORMATION_MAPPING.md`
  - `README.md` / `DEVELOPMENT.md` when developer workflow changes

## Change Rules
- Do not edit generated artifacts (`target/`, `logs/`, `dbt_packages/`).
- Keep `dbt_project.yml` selectors/tags aligned with model folder organization.
