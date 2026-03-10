---
title: Deployment
description: Docker Compose, Kubernetes, AKS, and Scaleway deployment guides.
order: 3
---

## Deployment Targets

- Local Docker Compose (recommended for most development)
- Local Kubernetes on kind (dev-like)
- Azure Kubernetes Service (AKS, dev-like)
- Scaleway Kapsule

## 1) Docker Compose (Local)

### Prerequisites
- Docker Engine + Compose plugin
- `.env` configured from `.env.template`
- For Superset map dashboards, set `MAPBOX_API_KEY` in `.env`

### Bring up stack
```bash
docker compose up -d
```

### Bring up bare minimum stack
Use this to mirror the minimal Terraform deployment scope locally (no DataHub, no heavy observability, no jupyter).

```bash
make compose-up-minimal
```

### Full bootstrap (recommended)
This sets up/validates env, starts services, and bootstraps MinIO/Superset/DataHub/warehouse assets.

```bash
./scripts/platform/bootstrap_all.sh --auto-fill-env
```

### Key local endpoints

| Service | URL |
|---------|-----|
| Airflow | `http://localhost:8080` |
| Superset | `http://localhost:8088` |
| DataHub | `http://localhost:9002` |
| MinIO API | `http://localhost:9000` |
| MinIO Console | `http://localhost:9001` |
| JupyterLab | `http://localhost:8888` |
| Grafana | `http://localhost:3001` |
| Prometheus | `http://localhost:9090` |

### Verify observability ingestion

```bash
make observability-verify
```

This verifies Grafana, Loki, Prometheus health, scrape status, Airflow metrics, OTLP trace ingestion, and log presence.

## 2) Kubernetes on kind (Dev-like)

### Prerequisites
- `kind`, `kubectl`, Docker
- `.env` in repository root

### Start
```bash
make k8s-dev-up
```

### Full Compose Parity on kind
To run the full Compose-equivalent stack in Kubernetes:

```bash
make k8s-dev-up-full
```

### Access via port-forward
```bash
kubectl -n odp-dev port-forward svc/airflow-webserver 8080:8080
kubectl -n odp-dev port-forward svc/minio 9000:9000 9001:9001
kubectl -n odp-dev port-forward svc/warehouse 5433:5432
```

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

This handles provisioning (RG + ACR + AKS), ingress-nginx + cert-manager, DNS/TLS, image build/push, Key Vault secret sync, core + parity rollout, and post-deploy smoke tests.

### Common overrides
```bash
AKS_RESOURCE_GROUP=ai-trial-rg \
AKS_CLUSTER_NAME=ai-trial-aks \
AKS_LOCATION=westeurope \
AKS_NODE_COUNT=4 \
FRONTEND_DOMAIN=example.com \
make k8s-aks-up
```

### Image-only update
```bash
make k8s-aks-update-images
```

### Teardown
```bash
make k8s-aks-down
```

## 4) Scaleway Kapsule

### Provision and deploy
```bash
make scaleway-redeploy-all
```

Set `DRY_RUN=true` for Terraform plan-only. Use `SKIP_TERRAFORM_APPLY=true`, `SKIP_DEPLOY=true`, `SKIP_SMOKE=true`, or `SKIP_IMAGE_BUILD=true` for partial runs.

### Teardown
```bash
make scaleway-destroy-all
```

## Environment and Secrets

All deployment modes depend on environment variables in `.env`.

Security-sensitive requirements:
- `MINIO_SSO_BRIDGE_SESSION_SECRET` must be a strong non-placeholder secret (32+ chars)
- `SUPERSET_OAUTH_DEFAULT_ROLE` defaults to least privilege (`Gamma`)
- AKS uses Azure Key Vault for secret management by default

## Deployment Notes

- Current Kubernetes manifests are dev-like, not production hardened
- For production readiness, add persistent volumes, network policies, secret manager integration, and hardened TLS/authz
