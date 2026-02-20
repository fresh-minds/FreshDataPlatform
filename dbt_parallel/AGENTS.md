# dbt Agent Instructions

Scope: `dbt_parallel/` (dbt models, snapshots, macros, project config).

## Must Do
- Keep model layering consistent:
  - `models/staging/` -> `stg_*`
  - `models/intermediate/` -> `int_*`
  - `models/marts/` -> `dim_*`, `fact_*`
- Update YAML definitions when model/source names or columns change.
- Run and report relevant checks after changes:
  - `.venv/bin/dbt debug --project-dir dbt_parallel --profiles-dir dbt_parallel`
  - `.venv/bin/dbt run --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'`
  - `.venv/bin/dbt test --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'`

## Documentation Requirements
- If transformations, lineage mapping, or run steps change, update:
  - `dbt_parallel/README.md`
  - `dbt_parallel/TRANSFORMATION_MAPPING.md`
  - `README.md` / `DEVELOPMENT.md` when developer workflow changes

## Change Rules
- Do not edit generated artifacts (`target/`, `logs/`, `dbt_packages/`).
- Keep `dbt_project.yml` selectors/tags aligned with model folder organization.
