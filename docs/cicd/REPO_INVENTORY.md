# CI/CD Repo Inventory

## Languages & Frameworks
- Python (Airflow, dbt, data pipelines, scripts)
- JavaScript (React + Vite frontend)

## Build Systems & Package Managers
- Python: `pyproject.toml` (setuptools), `pip`, `Makefile`
- Node: `npm` (`frontend/package.json`)

## Tests
- `pytest` (unit, QA, E2E markers)
- Playwright used for SSO E2E
- E2E scripts in `scripts/` (e.g., `run_e2e_tests.sh`, `run_sso_tests.sh`)

## Containers
- Dockerfiles:
  - `airflow/Dockerfile`
  - `frontend/Dockerfile`
  - `frontend/Dockerfile.k8s`
  - `notebooks/Dockerfile`
  - `ops/minio-sso-bridge/Dockerfile`
- Compose: `docker-compose.yml`, `docker-compose.backup.yml`

## Infrastructure-as-Code
- Kubernetes manifests + Kustomize:
  - `k8s/dev/`
  - `k8s/aks/`
- No Terraform/Bicep/Pulumi detected.

## Existing CI/CD
- GitHub Actions:
  - `.github/workflows/e2e-data-platform.yml`
  - `.github/workflows/schema-quality.yml`
  - `.github/workflows/sso-e2e.yml`

## Deployment Targets
- Kubernetes (AKS manifests in `k8s/aks/`)
- Local/dev stack via Docker Compose

## Secrets/Config
- `.env` and `.env.template` (local config)
- CI uses env vars in workflows (test credentials, service config)
