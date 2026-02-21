# Deployment Guide

## Deployment Targets
- Local Docker Compose (recommended for most development)
- Local Kubernetes on kind (dev-like)
- Azure Kubernetes Service (AKS, dev-like)

## 1) Docker Compose (Local)
### Prerequisites
- Docker Engine + Compose plugin
- `.env` configured from `.env.template`
- For `source_sp1_vacatures_ingestion`: set
  `SP1_USERNAME` and `SP1_PASSWORD` in `.env`

### Bring up stack
```bash
docker compose up -d
```

### Full bootstrap (recommended)
This sets up/validates env, starts services, and bootstraps MinIO/Superset/DataHub/warehouse assets.

```bash
./scripts/platform/bootstrap_all.sh --auto-fill-env
```
The script auto-creates `.venv` for bootstrap dependencies (`.[dev,pipeline]`) and recreates it if the Python interpreter path is stale.
Pass `--skip-dev-install` if you already manage a separate environment.

### Key local endpoints
- Airflow: `http://localhost:8080`
- Superset: `http://localhost:8088`
- DataHub: `http://localhost:9002`
- MinIO API: `http://localhost:9000`
- MinIO Console: `http://localhost:9001`
- JupyterLab: `http://localhost:8888`
- Grafana: `http://localhost:3001`
- Prometheus: `http://localhost:9090`

## 2) Kubernetes on kind (Dev-like)
### Prerequisites
- `kind`, `kubectl`, Docker
- `.env` in repository root

### Start
```bash
make k8s-dev-up
```

### Access via port-forward
```bash
kubectl -n odp-dev port-forward svc/airflow-webserver 8080:8080
kubectl -n odp-dev port-forward svc/minio 9000:9000 9001:9001
kubectl -n odp-dev port-forward svc/warehouse 5433:5432
kubectl -n odp-dev port-forward svc/keycloak 8090:8090
```

### Stop
```bash
make k8s-dev-down
```

### Full Compose Parity on kind
To run the full Compose-equivalent stack in Kubernetes (`docker-compose.yml` + k8s overrides):

```bash
make k8s-dev-up-full
```

This includes the core stack plus Superset, DataHub, observability components, portal, notebooks, and exporters.
On `arm64` kind clusters, `prometheus-msteams` is skipped automatically because its image is `amd64`-only.

### Shared SSO Gateway on kind
To front multiple UIs with one Keycloak-backed login session:

```bash
make k8s-sso-gateway-up
make k8s-sso-gateway-forward
```

Use host-based URLs such as:
- `http://airflow.localtest.me:8085`
- `http://superset.localtest.me:8085`
- `http://datahub.localtest.me:8085`
- `http://minio.localtest.me:8085`

## 3) AKS (Dev-like)
### Prerequisites
- Azure CLI (`az`) authenticated
- `kubectl`, `docker buildx`
- Azure subscription permissions for RG/AKS/ACR/DNS/Ingress resources
- `.env` configured

### Provision and deploy
```bash
make k8s-aks-up
```

This process handles:
- Resource group + ACR + AKS provisioning
- Airflow/frontend image build and push
- ingress-nginx + cert-manager install
- DNS records and TLS wiring
- Kubernetes manifests apply (`k8s/aks/`)

### Common overrides
```bash
AKS_RESOURCE_GROUP=ai-trial-rg \
AKS_CLUSTER_NAME=ai-trial-aks \
AKS_LOCATION=westeurope \
FRONTEND_DOMAIN=example.com \
make k8s-aks-up
```

### Teardown
```bash
make k8s-aks-down
```

Optional destructive flags (via script env vars):
- `DELETE_AKS_CLUSTER=true`
- `DELETE_ACR=true`
- `DELETE_INGRESS_PIP=true`
- `DELETE_DNS_RECORDS=true`
- `DELETE_RESOURCE_GROUP=true`

## Environment and Secrets
All deployment modes depend on environment variables in `.env`.

Minimum critical groups:
- Airflow DB/admin credentials
- Warehouse credentials
- MinIO credentials
- Superset and DataHub secrets
- Keycloak OIDC client credentials (if SSO enabled)

Security defaults:
- Keep `AIRFLOW_OAUTH_DEFAULT_ROLE=Viewer` unless you explicitly require a broader default.
- Keep Keycloak realm `registrationAllowed=false` in shared/dev-like environments.

Use generated strong values for secrets before any shared environment deployment.

## CI/CD Workflows
GitHub Actions currently include:
<!-- - `.github/workflows/ci.yml` -->
- `.github/workflows/security.yml`
<!-- - `.github/workflows/release.yml` -->
<!-- - `.github/workflows/cd-deploy.yml` -->
<!-- - `.github/workflows/build-images.yml` -->
- `.github/workflows/dbt-ci.yml`
<!-- - `.github/workflows/e2e-data-platform.yml` -->
<!-- - `.github/workflows/sso-e2e.yml` -->
- `.github/workflows/schema-quality.yml`

These validate:
- dbt + QA suites + evidence output
- SSO/browser/API security flows
- schema and governance consistency

## Deployment Notes
- Current Kubernetes manifests are explicitly dev-like, not production hardened.
- Persistence defaults are limited in the Kubernetes iteration.
- For production readiness, add:
  - persistent volumes and backup policy
  - stricter network policies
  - secret manager integration
  - hardened TLS, authz, and service exposure controls
