# Kubernetes Dev-Like (Phase A)

This folder contains a dev-like Kubernetes first iteration for the local stack:

- Airflow webserver + scheduler + init job
- Airflow metadata Postgres
- Warehouse Postgres
- MinIO + bucket init job (`bronze`, `silver`, `gold`)

## Local kind Prerequisites

- Docker
- [kind](https://kind.sigs.k8s.io/)
- kubectl
- `.env` present in repo root

## Start (kind)

From repo root:

```bash
make k8s-dev-up
```

What this does:

1. Creates a local kind cluster (`ai-trial-dev`) and mounts your repo into the node at `/workspace/ai_trial`.
2. Builds and loads the Airflow image (`ai-trial/airflow:dev`).
3. Creates/updates Kubernetes Secret `odp-env` from `.env`.
4. Applies core services, runs init Jobs, then starts Airflow.

## Full Compose Parity (kind)

If you want the full `docker-compose.yml` stack on local Kubernetes (core + Superset + DataHub + observability + portal + notebooks):

```bash
make k8s-dev-up-full
```

This command:

1. Bootstraps the core stack (`make k8s-dev-up`).
2. Builds and loads additional local images (`portal`, `jupyter`, `minio-sso-bridge`).
3. Converts `docker-compose.yml` to Kubernetes manifests (with `docker-compose.k8s.yml` overrides).
4. Applies the additional services to the same namespace.
5. Runs DataHub setup jobs (`mysql`, `elasticsearch`, `kafka`, `upgrade`) before finalizing GMS/frontend rollout.

Note: on `arm64` kind nodes, `prometheus-msteams` is skipped because the upstream image is `amd64`-only.

## Access (kind)

Use port-forward in separate terminals:

```bash
kubectl -n odp-dev port-forward svc/airflow-webserver 8080:8080
kubectl -n odp-dev port-forward svc/keycloak 8090:8090
kubectl -n odp-dev port-forward svc/minio 9000:9000 9001:9001
kubectl -n odp-dev port-forward svc/warehouse 5433:5432
```

Then:

- Airflow: [http://localhost:8080](http://localhost:8080)
- Keycloak: [http://localhost:8090](http://localhost:8090)
- MinIO API: [http://localhost:9000](http://localhost:9000)
- MinIO Console: [http://localhost:9001](http://localhost:9001)
- Warehouse Postgres: `localhost:5433`

## Shared SSO Gateway (kind)

To enforce one Keycloak login flow across all UIs behind a single auth gateway:

```bash
make k8s-sso-gateway-up
make k8s-sso-gateway-forward
```

Then use these ingress URLs (all protected by oauth2-proxy + Keycloak):

- Auth start: `http://auth.localtest.me:8085/oauth2/sign_in`
- Airflow: `http://airflow.localtest.me:8085`
- Superset: `http://superset.localtest.me:8085`
- DataHub: `http://datahub.localtest.me:8085`
- MinIO Console: `http://minio.localtest.me:8085`
- Portal: `http://portal.localtest.me:8085`
- Grafana: `http://grafana.localtest.me:8085`
- Prometheus: `http://prometheus.localtest.me:8085`
- Jupyter: `http://jupyter.localtest.me:8085`

Keycloak stays directly reachable at:

- `http://keycloak.localtest.me:8085`

To stop the gateway port-forward:

```bash
make k8s-sso-gateway-forward-stop
```

## Stop (kind)

```bash
make k8s-dev-down
```

This removes the namespace and deletes the kind cluster.

## AKS Deployment

You can deploy the same dev-like stack to Azure Kubernetes Service (AKS):

```bash
make k8s-aks-up
```

To tear it down again:

```bash
make k8s-aks-down
```

What this does:

1. Creates/updates an Azure resource group, ACR, and AKS cluster.
2. Builds and pushes the Airflow image to ACR.
3. Builds and pushes the frontend image to ACR.
4. Installs ingress-nginx + cert-manager.
5. Creates/updates Kubernetes Secret `odp-env` from `.env`.
6. Applies AKS-safe manifests from `k8s/aks` (no `hostPath` mounts).
7. Runs init Jobs and starts Airflow.
8. Exposes the frontend and core UIs publicly over HTTPS (DNS + Let's Encrypt):
   - `https://FRONTEND_DOMAIN` (frontend)
   - `https://airflow.FRONTEND_DOMAIN` (Airflow UI)
   - `https://minio.FRONTEND_DOMAIN` (MinIO Console)
   - `https://minio-api.FRONTEND_DOMAIN` (MinIO API)

Common overrides:

```bash
AKS_RESOURCE_GROUP=ai-trial-rg \
AKS_CLUSTER_NAME=ai-trial-aks \
AKS_LOCATION=westeurope \
AKS_NODE_VM_SIZE=Standard_B2s \
AKS_FORCE_ATTACH_ACR=false \
NAMESPACE=odp-dev \
make k8s-aks-up
```

Access (same as kind, after `az aks get-credentials` is configured by the script):

```bash
kubectl -n odp-dev port-forward svc/airflow-webserver 8080:8080
kubectl -n odp-dev port-forward svc/minio 9000:9000 9001:9001
kubectl -n odp-dev port-forward svc/warehouse 5433:5432
```

Public ingress routes after deployment:
- `https://FRONTEND_DOMAIN`
- `https://airflow.FRONTEND_DOMAIN`
- `https://minio.FRONTEND_DOMAIN`
- `https://minio-api.FRONTEND_DOMAIN`
- `https://keycloak.FRONTEND_DOMAIN`

## SSO Notes

Local kind:
- Add `127.0.0.1 keycloak` to `/etc/hosts` so the browser can resolve the same hostname the cluster uses.
- Ensure `.env` includes `KEYCLOAK_*` values and `MINIO_OIDC_REDIRECT_URI=http://localhost:9001/oauth_callback`.

AKS:
- Set `KEYCLOAK_OIDC_BASE_URL`, `KEYCLOAK_OIDC_AUTHORIZE_URL`, `KEYCLOAK_OIDC_TOKEN_URL`,
  and `KEYCLOAK_OIDC_DISCOVERY_URL` to the public Keycloak hostname (for example:
  `https://keycloak.FRONTEND_DOMAIN/realms/odp/protocol/openid-connect`).
- Set `MINIO_OIDC_REDIRECT_URI=https://minio.FRONTEND_DOMAIN/oauth_callback`.

## AKS Ingress + TLS (Custom Domain)

`make k8s-aks-up` handles this end-to-end. Under the hood it:

1. Installs ingress-nginx on AKS (cloud provider manifest) with a static Public IP.
2. Installs cert-manager and applies:
   - `k8s/aks/frontend.yaml` (frontend service as `ClusterIP`)
   - `k8s/aks/cert-issuer-letsencrypt-prod.yaml`
   - `k8s/aks/frontend-ingress.yaml`
3. Points Azure DNS records to the ingress public IP:
   - `FRONTEND_DOMAIN` -> ingress IP
   - `www`, `airflow`, `minio`, `minio-api`, `keycloak` -> CNAME to `FRONTEND_DOMAIN`

Important:
- The certificate remains `pending` until DNS resolves to the ingress IP.
- cert-manager will issue/update `frontend-tls` automatically once propagation completes.

## Notes

- This is intentionally **dev-like**, not production-grade.
- kind mode uses a host-mounted repo path so DAG/code changes are visible without rebuilding every time.
- AKS mode bakes DAG/project code into the Airflow image.
- Database/object-store state is ephemeral in this first iteration (pods use `emptyDir`).
