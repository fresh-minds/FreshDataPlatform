# ODP Staffing Demand Domain Rename Plan

Summary:
- This migration retired the legacy domain naming and standardizes the platform on `odp_staffing_demand`.
- The rollout was executed in three phases: compatibility, cutover, and legacy cleanup.
- Phase 3 is now complete for runtime commands, scripts, DAG entrypoints, contracts, and dbt model paths.

## Phase 1: Compatibility Layer (Completed)

Scope:
- Introduce canonical ODP Staffing Demand command and script names.
- Keep compatibility aliases while downstream references are migrated.
- Add the renamed Spark toggle used by canonical entrypoints.

Delivered:
- Canonical commands:
  - `make run-odp-staffing-demand`
  - `make run-odp-staffing-demand-metadata`
- Canonical scripts:
  - `scripts/pipeline/run_odp_staffing_demand_pipeline.py`
  - `scripts/pipeline/run_odp_staffing_demand_metadata_pipeline.py`
- Transitional compatibility aliases and wrappers were available during this phase.

## Phase 2: Technical Domain Cutover (Completed)

Scope:
- Switch orchestration, contracts, and transformation paths to canonical domain naming.
- Promote canonical DAG/package/model paths.

Delivered:
- Canonical DAG is active:
  - `dags/odp_staffing_demand_dag.py`
  - DAG ID: `odp_staffing_demand_pipeline`
- Legacy DAG shim removed:
  - Legacy compatibility DAG file deleted
- Canonical pipeline package promoted:
  - `pipelines/odp_staffing_demand/`
  - Legacy pipeline package deleted
- Canonical dbt model paths promoted:
  - `dbt/models/bronze/odp_staffing_demand/`
  - `dbt/models/silver/odp_staffing_demand/`
  - `dbt/models/gold/odp_staffing_demand/`
- Canonical schema contracts promoted:
  - `schema/models/odp_staffing_demand/`

Verification used during phase:
```bash
make schema-validate
make governance-validate
make dq-list
python3 scripts/testing/verify_dag_structure.py
.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
```

## Phase 3: Legacy Removal and Cleanup (Completed)

Scope:
- Remove compatibility aliases and legacy script wrappers.
- Remove deprecated env-var fallback reads.
- Normalize docs to canonical command/script names.

Delivered:
- Deprecated Make aliases removed:
  - Legacy run aliases removed
- Legacy wrappers removed:
  - Legacy pipeline wrapper scripts removed
- Deprecated fallback removed:
  - Deprecated Spark-toggle fallback read removed from `scripts/platform/bootstrap_all.sh`
- Env template cleaned:
  - Removed deprecated Spark toggle from `.env.template`
- Documentation updated to canonical paths and commands.

Verification for phase completion:
```bash
python3 -m py_compile \
  scripts/pipeline/run_odp_staffing_demand_pipeline.py \
  scripts/pipeline/run_odp_staffing_demand_metadata_pipeline.py

make schema-validate
make governance-validate
make dq-list
python3 scripts/testing/verify_dag_structure.py
.venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
```

## Notes

- `dbt debug` still depends on a reachable local warehouse and may fail if the target database is not provisioned.
- Phase completion here refers to code and configuration cleanup in this repository.

## Rollback Strategy

- Restore deleted compatibility aliases/scripts from git history if operationally required.
- Re-introduce temporary alias targets only if external automation still depends on legacy names.
- Prefer forward-fixing dependent automation to canonical `odp_staffing_demand` names.
