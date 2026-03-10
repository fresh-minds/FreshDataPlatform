---
title: CI/CD
description: Pipeline decisions, runbooks, required checks, and assumptions.
order: 7
---

## Pipeline Platform

GitHub Actions is used for all CI/CD workflows. Conventional Commits + Release Please handle automated versioning and changelog generation.

## CI Structure

| Workflow | Scope |
|----------|-------|
| `ci.yml` | Preflight, lint/format, typecheck, unit tests, integration smoke, build artifacts |
| `dbt-ci.yml` | dbt parse/compile, warehouse-backed dbt build/tests, schema + DAG checks |
| `security.yml` | SAST, secret scanning, dependency scans, container scanning, SBOM |
| `schema-quality.yml` | DBML validation |

## Security Tooling

| Tool | Purpose |
|------|---------|
| Gitleaks | Secret scanning |
| CodeQL | SAST (Python + JavaScript) |
| pip-audit | Python dependency scan |
| npm audit | Node dependency scan |
| Trivy | Container/filesystem scan |
| Syft | SBOM generation |

## Required Status Checks

- CI / preflight, lint-python, typecheck-python, lint-frontend, unit-tests, integration-smoke, build-python, build-frontend
- Security / gitleaks, codeql, python-deps, node-deps, trivy, sbom
- dbt CI / dbt compile & parse, dbt build & test, schema & DAG validation
- Schema Quality / validate-dbml

## Release Management

- Conventional Commits on `main` trigger Release Please to open release PRs
- On merge, the Release workflow creates a GitHub Release, builds artifacts, and signs Python distributions using Sigstore
- Container images are pushed to GitHub Container Registry (GHCR)

## Deployment Pipeline

Kubernetes deployments use Kustomize in `k8s/aks/`. Environment gates are enforced via GitHub environments (`dev`, `staging`, `prod`) with required reviewers.

## Runbooks

### Run CI Locally
```bash
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
make lint
make format-check
make type-check
pytest tests/ -m "not e2e and not pipeline_e2e"
```

### Run CI Frontend Checks
```bash
cd frontend
npm ci
npm run lint
npm run format:check
npm run build
```

### Cut a Release
1. Use Conventional Commits on `main`
2. Release Please opens a release PR automatically — merge it
3. The Release workflow creates a GitHub Release and builds artifacts

### Deploy to Dev
Merge to `main` triggers the CD workflow, which builds images, pushes to GHCR, and applies with Kustomize.

### Deploy to Staging/Prod
Trigger `CD Deploy` via workflow dispatch. Provide environment and image tags. Approvals are enforced via GitHub Environments.

### Rollback
```bash
kubectl rollout undo deployment/airflow-webserver -n <namespace>
kubectl rollout undo deployment/airflow-scheduler -n <namespace>
kubectl rollout undo deployment/frontend -n <namespace>
```

## Assumptions

- `CODEOWNERS` entries use placeholder GitHub teams and must be updated to real org teams/users
- AKS access is provided via GitHub OIDC with environment-scoped secrets
- Container images are pushed to GHCR using the repository owner namespace
- Frontend versioning is not synchronized with backend versioning; releases use a single repo version from `pyproject.toml`
