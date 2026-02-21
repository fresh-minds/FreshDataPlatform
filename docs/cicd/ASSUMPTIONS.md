# CI/CD Assumptions

- `CODEOWNERS` entries use placeholder GitHub teams and must be updated to real org teams/users.
- AKS access is provided via GitHub OIDC with environment-scoped secrets:
  - `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_SUBSCRIPTION_ID`
  - `AKS_RESOURCE_GROUP`, `AKS_CLUSTER_NAME`
- Container images are pushed to GHCR using the repository owner namespace.
- `k8s/aks/` is the deployment base for `staging` and `prod`. Environment-specific overlays do not yet exist; namespace and image overrides are applied at deploy time.
- `k8s/dev/` is intended for local/kind usage and is not used for CI CD unless explicitly requested.
- Frontend versioning is not synchronized with backend versioning; releases use a single repo version from `pyproject.toml`.
- Python type checking runs with the existing `mypy` configuration; missing dependency types may require future tightening.
- Integration coverage uses a smoke health-check against Docker Compose services; expand to full integration suites when available.
- `npm audit` is configured to fail on `high`/`critical` severity only to avoid blocking for known moderate issues.
