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

### Structured deployment logs

The full-parity script supports structured observability events for each Kompose stage.

Environment flags:
- `K8S_SCRIPT_LOG_FORMAT`: `text` (default), `json`, or `both`
- `K8S_SCRIPT_RUN_ID`: optional correlation ID for one deployment run

Examples:

```bash
K8S_SCRIPT_LOG_FORMAT=json make k8s-dev-up-full
```

```bash
K8S_SCRIPT_LOG_FORMAT=both K8S_SCRIPT_RUN_ID=kind-full-deploy-20260221 make k8s-dev-up-full
```

Quick validation:

```bash
K8S_SCRIPT_LOG_FORMAT=json make k8s-dev-up-full | grep '"event":"kompose_'
```

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

Prerequisite:
- Set `KEYCLOAK_GATEWAY_CLIENT_SECRET` in `.env` to a non-placeholder value before running `make k8s-sso-gateway-up` (the setup script now fails fast when this secret is missing or still `change_me*`).

Verification:

```bash
grep '^KEYCLOAK_GATEWAY_CLIENT_SECRET=' .env
kubectl -n odp-dev get secret odp-env -o jsonpath='{.data.KEYCLOAK_GATEWAY_CLIENT_SECRET}' | base64 --decode; echo
```

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

AKS secret prerequisites (default path):
- Your Azure principal can create/manage Key Vault in the target resource group.
- Your Azure principal can create role assignments on the Key Vault scope.

By default, `make k8s-aks-up` runs `make k8s-aks-smoke` automatically at the end (`AKS_SMOKE_AFTER_UP` unset/empty = `true`) and fails if smoke checks fail.
To skip smoke checks for a run:

```bash
AKS_SMOKE_AFTER_UP=false make k8s-aks-up
```

For faster incremental app rollouts (without infra/parity re-apply):

```bash
make k8s-aks-update-images
```

To patch only specific services:

```bash
AKS_IMAGES=frontend,portal-api make k8s-aks-update-images
```

`make k8s-aks-update-images` expects deployments to already exist and skips missing deployments in the selected namespace.

To tear it down again:

```bash
make k8s-aks-down
```

What this does:

- **Provisioning and ingress**
   1. Creates/updates an Azure resource group, ACR, and AKS cluster.
   2. Installs ingress-nginx + cert-manager.
   3. Wires DNS + TLS for public HTTPS endpoints.

- **Image build and publish**
   1. Builds and pushes the Airflow image to ACR.
   2. Builds and pushes the frontend image to ACR.
   3. Builds and pushes additional parity-service images (for example `portal-api`, `jupyter`, `minio-sso-bridge`) when enabled in the AKS flow.

- **Core and parity rollout**
   1. Creates/updates Azure Key Vault (default), syncs `.env` entries into Key Vault, then syncs Kubernetes Secret `odp-env` via CSI provider.
   2. Applies AKS-safe manifests from `k8s/aks` (no `hostPath` mounts).
   3. Runs init jobs and starts Airflow.
   4. Applies parity manifests generated from `docker-compose.yml` + `docker-compose.k8s.yml`, including staged DataHub dependencies and setup jobs.
   5. Deploys `dbt-docs` as a dedicated service exposed via ingress.

- **Airflow reliability hardening**
  - Uses tuned startup/readiness/liveness probe behavior for metadata Postgres and webserver/scheduler.
  - Applies guarded retry behavior around metadata initialization to avoid startup dead-ends.
  - Uses conservative rollout settings to reduce AKS transient `503` windows.

- **DataHub and auth reliability hardening**
  - Stages DataHub setup before GMS/frontend rollout.
  - Applies one-time MySQL self-heal and setup-job replay when GMS reports `Unknown database 'datahub'`.
  - Uses hardened GMS probe behavior with extended startup budget for slow cold starts (socket-based startup probe, `/health` readiness/liveness).
  - Aligns Kafka startup/readiness/liveness probes with its internal listener (`29092`) and uses longer probe command timeouts on AKS to avoid false-negative startup checks.
  - Applies explicit Kafka AKS resources (`requests: 300m/1200Mi`, `limits: 1000m/2048Mi`) to reduce memory-pressure evictions during parity rollout.
  - Forces `publishNotReadyAddresses=true` on `datahub-kafka` Service so broker self-connect via service DNS works before readiness flips to true.
  - Uses dedicated DataHub ingress buffering to avoid OIDC header-size `502` issues.

- **Config safety and URL consistency**
  - Rewrites browser-facing service/auth URLs to `https://*.FRONTEND_DOMAIN` for AKS parity manifests.
  - Injects observability configs (`alertmanager`, `loki`, `prometheus`, `promtail`, `tempo`, `otel-collector`, `grafana`) via ConfigMaps for AKS-safe file mounting.
  - Reconciles Keycloak config as part of the AKS flow to keep client redirects aligned.
  - Validates manifest placeholders before apply.
  - Rollout-failure diagnostics resolve pod selectors from each Deployment (`spec.selector.matchLabels`) so Kompose-labeled workloads dump the correct failing pod info.

- **Public entrypoints**
   - `https://FRONTEND_DOMAIN` (frontend)
   - `https://airflow.FRONTEND_DOMAIN` (Airflow UI)
   - `https://minio.FRONTEND_DOMAIN` (MinIO Console)
   - `https://minio-api.FRONTEND_DOMAIN` (MinIO API)

AKS parity mode deploys the full `docker-compose.yml` resource set (portal-api,
DataHub, Superset, Jupyter, observability stack, exporters, and dbt docs)
after the core stack is healthy.

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

AKS Key Vault overrides:

```bash
AKS_KEY_VAULT_NAME=aitrialkv1234abcd \
AKS_KEY_VAULT_RESOURCE_GROUP=ai-trial-rg \
make k8s-aks-up
```

Disable AKS Key Vault secret sync (fallback to direct `.env` -> Kubernetes secret):

```bash
AKS_USE_KEY_VAULT=false make k8s-aks-up
```

Constraint:
- The AKS manifests currently reference Kubernetes secret name `odp-env`; overriding `AKS_KEY_VAULT_SECRET_NAME` is not supported.

DataHub GMS startup/resource overrides (optional, useful for slow AKS cold starts):

```bash
DATAHUB_GMS_STARTUP_FAILURE_THRESHOLD=300 \
DATAHUB_GMS_LIVENESS_INITIAL_DELAY_SECONDS=900 \
DATAHUB_GMS_CPU_REQUEST=500m \
DATAHUB_GMS_MEMORY_REQUEST=1500Mi \
DATAHUB_GMS_CPU_LIMIT=2 \
DATAHUB_GMS_MEMORY_LIMIT=3Gi \
make k8s-aks-up
```

Verification after rollout:

```bash
make k8s-aks-smoke
kubectl -n odp-dev get secretproviderclass odp-env-keyvault
kubectl -n odp-dev rollout status deployment/odp-env-keyvault-sync --timeout=600s
kubectl -n odp-dev rollout status deployment/datahub-gms --timeout=1200s
kubectl -n odp-dev rollout status deployment/datahub-kafka --timeout=1200s
kubectl -n odp-dev get pods -l io.kompose.service=datahub-kafka -o wide
kubectl -n odp-dev describe pod -l io.kompose.service=datahub-gms | sed -n '1,220p'
kubectl -n odp-dev logs deploy/datahub-gms --tail=200
kubectl -n odp-dev get deploy datahub-gms -o jsonpath='{.spec.template.spec.containers[0].startupProbe}' && echo
```

`make k8s-aks-smoke` runs in-cluster endpoint checks for observability and core services, retries each HTTP check for roughly 60 seconds to absorb short warm-up windows, then prints a GREEN/RED/SKIP matrix and exits non-zero on failures.

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
- `https://datahub.FRONTEND_DOMAIN`
- `https://superset.FRONTEND_DOMAIN`
- `https://grafana.FRONTEND_DOMAIN`
- `https://jupyter.FRONTEND_DOMAIN`
- `https://prometheus.FRONTEND_DOMAIN`
- `https://alertmanager.FRONTEND_DOMAIN`
- `https://dbt-docs.FRONTEND_DOMAIN`
- `https://portal-api.FRONTEND_DOMAIN`

## SSO Notes

Local kind:
- Add `127.0.0.1 keycloak` to `/etc/hosts` so the browser can resolve the same hostname the cluster uses.
- Ensure `.env` includes `KEYCLOAK_*` values and `MINIO_OIDC_REDIRECT_URI=http://localhost:9001/oauth_callback`.
- Ensure `.env` includes a strong `MINIO_SSO_BRIDGE_SESSION_SECRET` (32+ chars, non-placeholder).

AKS:
- Set `KEYCLOAK_OIDC_BASE_URL`, `KEYCLOAK_OIDC_AUTHORIZE_URL`, `KEYCLOAK_OIDC_TOKEN_URL`,
  and `KEYCLOAK_OIDC_DISCOVERY_URL` to the public Keycloak hostname (for example:
  `https://keycloak.FRONTEND_DOMAIN/realms/odp/protocol/openid-connect`).
- Set `MINIO_OIDC_REDIRECT_URI=https://minio.FRONTEND_DOMAIN/oauth_callback`.
- MinIO login entrypoints `https://minio.FRONTEND_DOMAIN/login`, `/start`, and `/callback`
  are routed through `minio-sso-bridge`, so users with an existing Keycloak session are
  redirected straight back into the MinIO console.
- Verify ingress behavior:
  ```bash
  curl -sS -D - -o /dev/null "https://minio.${FRONTEND_DOMAIN}/login"
  ```
  Expect a `302` redirect to Keycloak (`/protocol/openid-connect/auth`).
- Realm self-registration is disabled by default in bundled manifests (`registrationAllowed: false`).
- Keep `AIRFLOW_OAUTH_DEFAULT_ROLE` at least privilege (`Viewer`) unless you have a controlled admin onboarding flow.

## AKS Ingress + TLS (Custom Domain)

`make k8s-aks-up` handles this end-to-end. Under the hood it:

1. Installs ingress-nginx on AKS (cloud provider manifest) with a static Public IP.
2. Installs cert-manager and applies:
   - `k8s/aks/frontend.yaml` (frontend service as `ClusterIP`)
   - `k8s/aks/cert-issuer-letsencrypt-prod.yaml`
   - `k8s/aks/frontend-ingress.yaml`
3. Points Azure DNS records to the ingress public IP:
   - `FRONTEND_DOMAIN` -> ingress IP
   - `www`, `airflow`, `minio`, `minio-api`, `keycloak`, `datahub`, `superset`, `grafana`, `jupyter`, `prometheus`, `alertmanager`, `dbt-docs`, `portal-api` -> CNAME to `FRONTEND_DOMAIN`

Important:
- The certificate remains `pending` until DNS resolves to the ingress IP.
- cert-manager will issue/update `frontend-tls` automatically once propagation completes.

## Notes

- This is intentionally **dev-like**, not production-grade.
- kind mode uses a host-mounted repo path so DAG/code changes are visible without rebuilding every time.
- AKS mode bakes DAG/project code into the Airflow image.
- Database/object-store state is ephemeral in this first iteration (pods use `emptyDir`).
