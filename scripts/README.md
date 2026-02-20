# Scripts Layout

This folder uses a domain-oriented layout. Scripts should be invoked via their
canonical subfolder paths.

## Canonical directories

- `scripts/catalog/`: DataHub and metadata catalog scripts.
- `scripts/minio/`: MinIO bucket/object utilities and fixture loading.
- `scripts/pipeline/`: Local pipeline runners and dbt orchestration helpers.
- `scripts/platform/`: Platform bootstrap, health checks, and alerting utilities.
- `scripts/quality/`: Schema, governance, and data-quality validators.
- `scripts/warehouse/`: Warehouse schema/security/introspection scripts.
- `scripts/sso/`: SSO/OIDC helpers and reporting.
- `scripts/superset/`: Superset setup/bootstrap/config assets.
- `scripts/testing/`: E2E/SSO/CI validation scripts.
- `scripts/k8s/`: kind/Kubernetes helper scripts.
- `scripts/aks/`: AKS provisioning/teardown scripts.

## Conventions for new scripts

- Put domain-specific scripts in the matching subfolder.
- If relocating an existing script, update Makefile/CI/docs references in the same change.
