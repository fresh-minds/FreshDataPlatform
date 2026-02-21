# Scripts Agent Instructions

Scope: `scripts/` and its subfolders.

## Must Do
- Keep domain scripts in canonical folders:
  - `scripts/aks/`
  - `scripts/catalog/`
  - `scripts/k8s/`
  - `scripts/minio/`
  - `scripts/pipeline/`
  - `scripts/platform/`
  - `scripts/quality/`
  - `scripts/sso/`
  - `scripts/superset/`
  - `scripts/testing/`
  - `scripts/warehouse/`
- After script path/name changes, update all references in the same change:
  - `Makefile`
  - `.github/workflows/*`
  - `README.md`
  - `DEVELOPMENT.md`
  - `scripts/README.md`

## Validation
- Python scripts: `python3 -m py_compile $(rg --files scripts | rg '\\.py$')`
- Shell scripts: validate with `bash -n <script>` for changed files.

## Documentation Requirements
- Any behavior or CLI argument changes must be reflected in docs and examples.
- Keep usage examples copy-pasteable and aligned with canonical script paths.
