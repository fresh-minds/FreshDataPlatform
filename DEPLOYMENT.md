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

### Verify observability ingestion (Docker Compose)

Run the built-in end-to-end verification:

```bash
make observability-verify
```

What it verifies:
- Grafana, Loki, and Prometheus health endpoints
- Grafana datasource connectivity for Prometheus and Loki
- Prometheus scrape status for core observability targets
- Presence of `airflow_*` metrics in Prometheus
- OTLP trace ingestion path (synthetic trace -> OTEL Collector -> Tempo trace query API)
- Presence of Airflow file logs (`job="airflow"`) in Loki
- Presence of Docker stdout logs (`job="docker"`) in Loki

Optional lookback window (seconds, default `900`):

```bash
OBS_LOOKBACK_SECONDS=1800 make observability-verify
```

Optional strict trace-volume mode (minimum spans over a time window):

```bash
OBS_REQUIRE_TRACE_VOLUME=true \
OBS_TRACE_VOLUME_WINDOW_SECONDS=30 \
OBS_TRACE_VOLUME_MIN_SPANS=10 \
make observability-verify
```

Notes:
- Strict mode uses Tempo counters directly.
- The verifier emits synthetic OTLP traces during the strict window and validates observed span increase meets the threshold.

Optional ambient trace-volume mode (no synthetic trace probes):

```bash
OBS_REQUIRE_TRACE_VOLUME=true \
OBS_TRACE_VOLUME_MODE=ambient \
OBS_TRACE_VOLUME_WINDOW_SECONDS=60 \
OBS_TRACE_VOLUME_MIN_SPANS=5 \
make observability-verify
```

Ambient mode validates naturally occurring trace activity from the platform during the window.
In ambient mode, the verifier does not inject synthetic traces.

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

### Deployment script observability logs
`make k8s-dev-up-full` now emits operation-level events from the shared Kompose pipeline.

Use plain text logs (default):

```bash
make k8s-dev-up-full
```

Use machine-parseable JSON logs for ingestion in log pipelines:

```bash
K8S_SCRIPT_LOG_FORMAT=json make k8s-dev-up-full
```

Correlate one run end-to-end with a custom run ID:

```bash
K8S_SCRIPT_LOG_FORMAT=both K8S_SCRIPT_RUN_ID=dev-rollout-001 make k8s-dev-up-full
```

Verification:
- Confirm JSON events exist: `K8S_SCRIPT_LOG_FORMAT=json make k8s-dev-up-full | head -n 20`
- Confirm event names appear: `K8S_SCRIPT_LOG_FORMAT=json make k8s-dev-up-full | grep 'kompose_generate\|kompose_fix_deployments'`

### Shared SSO Gateway on kind
To front multiple UIs with one Keycloak-backed login session:

Prerequisite:
- `KEYCLOAK_GATEWAY_CLIENT_SECRET` must be set to a non-placeholder value in `.env` (the gateway setup now fails fast if it is missing or still `change_me*`).

Verify before enabling the gateway:

```bash
grep '^KEYCLOAK_GATEWAY_CLIENT_SECRET=' .env
kubectl -n odp-dev get secret odp-env -o jsonpath='{.data.KEYCLOAK_GATEWAY_CLIENT_SECRET}' | base64 --decode; echo
```

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
- For AKS Key Vault sync: data-plane write access on the Key Vault (`Key Vault Secrets Officer` or `Key Vault Administrator`) and permission to create role assignments if you want the script to auto-grant missing access
- `.env` configured

### Provision and deploy
```bash
make k8s-aks-up
```

### Image-only update (faster incremental rollout)
Use this when AKS infra and workloads already exist and you only want to publish fresh app images and restart matching deployments.

```bash
make k8s-aks-update-images
```

This flow:
- Fetches AKS credentials and logs into ACR
- Builds/pushes selected images
- Patches existing deployments with new tags and waits for rollout

Constraints:
- Does **not** provision/update AKS infra, ingress, DNS, or parity manifests
- Skips deployments that do not exist in the target namespace
- Intended as a follow-up loop after at least one successful `make k8s-aks-up`

This process handles:
- **Provisioning and ingress**
  - Resource group + ACR + AKS provisioning
  - ingress-nginx + cert-manager install
  - DNS records and TLS wiring

- **Image build and publish**
  - Airflow/frontend image build and push
  - Additional image build and push for parity services (`portal-api`, `jupyter`, `minio-sso-bridge`)
  - AKS frontend image build injects public Vite values (`VITE_KEYCLOAK_URL`, `VITE_PORTAL_API_URL`, `VITE_DBT_DOCS_URL`) to avoid localhost redirects in static bundles

- **Core and parity rollout**
  - Azure Key Vault-backed secret flow for AKS: non-empty `.env` values are synced to Key Vault and then projected back into Kubernetes Secret `odp-env` via CSI (empty values are skipped because Key Vault does not allow empty secret values)
  - Core Kubernetes manifests apply (`k8s/aks/`)
  - Full `docker-compose.yml` parity apply via Kompose conversion (`docker-compose.yml` + `docker-compose.k8s.yml`)
  - DataHub dependency setup jobs and staged DataHub rollout
  - `dbt-docs` deployment rollout with init-time `dbt docs generate` so docs/lineage refresh each deploy
  - Warehouse Postgres self-heal on rerun (`rollout restart`) to recover from stale/corrupt ephemeral pod state

- **Airflow reliability hardening**
  - Airflow metadata Postgres uses password-based host auth (`POSTGRES_HOST_AUTH_METHOD=scram-sha-256`) in AKS/dev manifests
  - Airflow metadata Postgres uses tuned startup/readiness/liveness probes to avoid transient probe timeouts causing endpoint flapping and Airflow UI `503` responses during restarts
  - If Airflow webserver rollout detects an uninitialized metadata DB (`airflow db init` required), AKS deploy reruns `airflow-init` once and retries webserver/scheduler rollout
  - Airflow webserver uses a generous startup probe window to avoid premature liveness restarts while Gunicorn initializes on constrained AKS nodes
  - Airflow webserver Gunicorn is tuned for AKS dev-size nodes (`--workers 1 --timeout 300`) to avoid startup stalls that can surface as `503`
  - Airflow webserver sets `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300` and explicit Keycloak OIDC endpoint envs in AKS manifests so startup does not depend on stale/local secret defaults
  - Airflow webserver runs with `USE_DATAHUB=false` and a default `AIRFLOW_CONN_DATAHUB_REST_DEFAULT` in AKS to prevent plugin init failures from blocking UI startup
  - Airflow webserver deployment strategy is `Recreate` in AKS to prevent dual non-ready ReplicaSets from stalling rollout progress and keeping ingress on `503`
  - Airflow webserver and scheduler deployments set `revisionHistoryLimit=3` so old ReplicaSets are auto-pruned and rollout history does not accumulate indefinitely
  - Airflow scheduler liveness probe is tuned for AKS node variability (`initialDelay=120s`, `period=60s`, `timeout=60s`, `failureThreshold=5`) to avoid false restarts and stale scheduler heartbeat warnings
  - Airflow webserver deployment is recreated before apply to avoid historical `env.value`/`env.valueFrom` merge conflicts that can block reruns
  - Airflow OAuth auto-registration defaults to role `Viewer` (least privilege) for AKS deploys; if `AIRFLOW_OAUTH_DEFAULT_ROLE` is missing in `.env`, `k8s-aks-up` patches `odp-env` with `Viewer` and backfills that value to AKS Key Vault when Key Vault sync is enabled
  - Airflow init job wait timeout is independently configurable via `AIRFLOW_INIT_JOB_TIMEOUT` (default `960s`) so slower metadata migrations do not fail the full AKS rollout when global `WAIT_TIMEOUT` stays lower for normal deployment checks
  - AKS job waits re-check Kubernetes Job success/Complete state after a timeout response, so late-completing jobs (such as `airflow-init`) are not failed due to a `kubectl wait` race
  - Airflow webserver/scheduler rollout wait timeout is independently configurable via `AIRFLOW_DEPLOYMENT_TIMEOUT` (default `600s`) so startup probe warmup windows do not get cut off by a shorter global `WAIT_TIMEOUT`

- **DataHub and auth reliability hardening**
  - DataHub setup jobs are deleted with explicit wait-for-delete before recreation on reruns, preventing stale long-running Job pods from blocking full AKS redeploys
  - DataHub setup job wait timeout is configurable via `DATAHUB_SETUP_JOB_TIMEOUT` (default `1200s`) to tolerate slow image pulls on AKS nodes
  - `datahub-elasticsearch-setup` uses a dedicated shorter timeout via `DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT` (default `300s`) so AKS deploys fail fast on Elasticsearch setup regressions instead of waiting for the global DataHub setup timeout
  - `datahub-elasticsearch-setup` runs with `DATAHUB_ANALYTICS_ENABLED=false` in AKS manifests to avoid a known usage-event bootstrap script failure (`sed: /index/usage-event/datahub_usage_event: No such file or directory`) in `acryldata/datahub-elasticsearch-setup:v1.2.0.1`
  - Kompose post-processing enforces DataHub Elasticsearch `startupProbe` + `readinessProbe` + delayed `livenessProbe` on `:9200` so DataHub setup jobs run only after Elasticsearch is actually serving HTTP
  - Kompose post-processing enforces DataHub Kafka startup/readiness probes using `kafka-broker-api-versions --bootstrap-server localhost:29092` (instead of TCP-only checks) so `datahub-kafka-setup` starts only after broker metadata is available
  - DataHub Kafka AKS parity manifests now enforce explicit resources (`requests: 300m/1200Mi`, `limits: 1000m/2048Mi`) plus longer probe command timeouts (`startup/readiness timeoutSeconds=30`) to reduce memory-pressure evictions and false rollout timeouts on dev-size AKS nodes
  - DataHub Kafka deployment strategy is forced to `Recreate` on AKS to prevent overlapping pods with the same broker ID (`KAFKA_BROKER_ID=1`), which can cause metadata/bootstrap disconnect loops
  - DataHub Kafka Service is generated with `publishNotReadyAddresses=true` so broker self-connect via service DNS works during startup (prevents readiness deadlocks)
  - Kompose post-processing enforces DataHub MySQL startup/readiness/liveness probes with a longer startup budget, preventing liveness flapping that can block DataHub setup jobs
  - Kompose post-processing aligns DataHub Kafka listeners on AKS to in-cluster `PLAINTEXT://datahub-kafka:29092` only (no localhost-advertised listener), avoiding metadata resolution failures in `datahub-kafka-setup`
  - DataHub runtime components (`datahub-gms`, `datahub-upgrade`) use `DATAHUB_MYSQL_USER` / `DATAHUB_MYSQL_PASSWORD` instead of remote MySQL root credentials
  - If `datahub-gms` rollout fails with MySQL host-auth errors (`Host 'x.x.x.x' is not allowed to connect`), AKS deploy applies a one-time in-cluster MySQL grant self-heal for the DataHub app user on the DataHub schema and retries `datahub-gms`
  - If `datahub-gms` rollout fails with MySQL schema errors (`Unknown database 'datahub'`), AKS deploy applies a one-time in-cluster schema self-heal (creates `datahub` database, reapplies grants), reruns DataHub setup jobs, and retries `datahub-gms`
  - Kompose post-processing enforces DataHub GMS `startupProbe` + `readinessProbe` + relaxed `livenessProbe` with an extended cold-start budget (30 minutes + initial delay) to prevent premature restarts and transient OIDC callback failures (`Failed to provision user ...`) while GMS is still booting
  - DataHub uses a dedicated AKS ingress (`datahub-ingress.yaml`) with larger proxy response header buffers (`proxy-buffer-size=32k`, `proxy-buffers-number=8`) to prevent intermittent OIDC `/authenticate` `502` errors (`upstream sent too big header`) without changing buffer settings for other hosts
  - DataHub ingress uses a dedicated TLS secret (`datahub-tls`) so certificate SANs are isolated from the shared frontend host cert and ingress controller cert-validation warnings are avoided

- **Config safety and URL consistency**
  - Kompose-generated AKS deployments are post-processed to rewrite browser-facing auth/redirect URLs (portal, minio-sso-bridge, grafana, superset, datahub) to `https://*.${FRONTEND_DOMAIN}` instead of localhost defaults
  - AKS ingress routes MinIO `/login`, `/start`, and `/callback` through `minio-sso-bridge` so an existing Keycloak session can sign users directly into MinIO
  - Superset custom auth/bootstrap files are injected as Kubernetes ConfigMaps during AKS parity conversion (instead of hostPath bind mounts) so `superset_config.py` is always present and `/login` auto-redirects directly to Keycloak for existing SSO sessions
  - Alertmanager configuration is injected via Kubernetes `alertmanager-config` ConfigMap (`ops/observability/alertmanager.yml`) so AKS parity deploy does not depend on hostPath file mounts
  - Keycloak is part of the AKS core phase and is reapplied before full-stack parity so realm/client changes (including portal redirect URIs) are continuously reconciled
  - Portal frontend auth fallback is domain-aware: when `VITE_KEYCLOAK_URL` is not present at build-time, it derives `https://keycloak.<current-root-domain>` (while keeping `http://localhost:8090` for local hostnames)
  - AKS manifest rendering validates unresolved placeholders before apply, and job waits fail fast for `InvalidImageName`/image-pull errors to shorten troubleshooting loops
  - Deployment rollout diagnostics now resolve selectors from each Deployment (`spec.selector.matchLabels`) so Kompose-labeled workloads (for example `io.kompose.service=datahub-kafka`) print the correct pod diagnostics on failure

### Common overrides
```bash
AKS_RESOURCE_GROUP=ai-trial-rg \
AKS_CLUSTER_NAME=ai-trial-aks \
AKS_LOCATION=westeurope \
AKS_NODE_COUNT=4 \
FRONTEND_DOMAIN=example.com \
make k8s-aks-up
```

For existing clusters, `k8s-aks-up` reconciles the System nodepool to at least `AKS_NODE_COUNT` (scales node count directly or raises autoscaler min-count).

AKS Key Vault overrides:

```bash
AKS_KEY_VAULT_NAME=aitrialkv1234abcd \
AKS_KEY_VAULT_RESOURCE_GROUP=ai-trial-rg \
make k8s-aks-up
```

`make k8s-aks-up` is idempotent for the AKS Key Vault provider add-on: if it is already enabled, the script skips re-enabling instead of failing.

Optional Key Vault RBAC propagation tuning:

```bash
AKS_KEY_VAULT_SECRET_SET_RETRIES=24 \
AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS=10 \
make k8s-aks-up
```

Disable AKS Key Vault secret sync (fallback to direct `.env` -> Kubernetes secret):

```bash
AKS_USE_KEY_VAULT=false make k8s-aks-up
```

Portal assistant Foundry auth (AKS recommendation):

- Primary agent reference vars: `AZURE_EXISTING_AIPROJECT_ENDPOINT` and `AZURE_EXISTING_AGENT_ID` (or `AZURE_EXISTING_AGENT_NAME`).
- Legacy aliases remain supported: `AZURE_FOUNDRY_AGENT_ENDPOINT` and `AZURE_FOUNDRY_AGENT_ID` (or `AZURE_FOUNDRY_AGENT_NAME`).
- `portal-api` uses API key auth when `AZURE_FOUNDRY_API_KEY` is set.
- If `AZURE_FOUNDRY_API_KEY` is empty, `portal-api` uses `DefaultAzureCredential`.
- For containerized local runs with `DefaultAzureCredential`, provide service principal env vars (`AZURE_TENANT_ID`, `AZURE_CLIENT_ID`, `AZURE_CLIENT_SECRET`).
- For AKS parity deploys, portal-api reads these values from Kubernetes secret `odp-env` (synced from AKS Key Vault by default; fallback is direct `.env` -> Kubernetes secret when `AKS_USE_KEY_VAULT=false`).
- `portal-api` and AKS Key Vault sync both normalize one wrapping quote pair for these credentials, so values like `'tenant-guid'` are treated as `tenant-guid`.
- After changing these values in `.env`, rerun `make k8s-aks-up` or recreate portal-api pods so new secret-backed env values are picked up.

Example:

```bash
AZURE_EXISTING_AIPROJECT_ENDPOINT=https://<your-project>.services.ai.azure.com/api/projects/<project-name> \
AZURE_EXISTING_AGENT_ID=<agent-name>:1 \
AZURE_TENANT_ID=<tenant-id> \
AZURE_CLIENT_ID=<client-id> \
AZURE_CLIENT_SECRET=<client-secret> \
make k8s-aks-up
```

Fast refresh for portal-api after secret update:

```bash
kubectl --context <aks-context> -n <namespace> rollout restart deployment/portal-api
kubectl --context <aks-context> -n <namespace> rollout status deployment/portal-api --timeout=600s
```

AKS rollout resilience overrides (for transient DNS/API watch interruptions):

```bash
AKS_WAIT_RETRIES=8 \
AKS_WAIT_RETRY_DELAY_SECONDS=15 \
make k8s-aks-up
```

Airflow init timeout override (for slower AKS nodes):

```bash
AIRFLOW_INIT_JOB_TIMEOUT=1500s \
make k8s-aks-up
```

Airflow deployment timeout override (for slower webserver warmup):

```bash
AIRFLOW_DEPLOYMENT_TIMEOUT=1200s \
make k8s-aks-up
```

DataHub Elasticsearch setup timeout override (fail fast on setup errors):

```bash
DATAHUB_ELASTICSEARCH_SETUP_JOB_TIMEOUT=300s \
make k8s-aks-up
```

Image-only update overrides (target a subset and tune rollout wait):

```bash
AKS_IMAGES=frontend,portal-api \
AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT=900s \
make k8s-aks-update-images
```

Supported `AKS_IMAGES` values: `airflow`, `frontend`, `portal-api`, `jupyter`, `minio-sso-bridge`.

### Teardown
```bash
make k8s-aks-down
```

Optional destructive flags (via script env vars):
- `DELETE_AKS_CLUSTER=true`
- `DELETE_ACR=true`
- `DELETE_INGRESS_PIP=true`
- `DELETE_DNS_RECORDS=true`
- `DELETE_KEY_VAULT=true`
- `PURGE_KEY_VAULT=true`
- `DELETE_RESOURCE_GROUP=true`

## Environment and Secrets
All deployment modes depend on environment variables in `.env`.

Security-sensitive requirements:
- `MINIO_SSO_BRIDGE_SESSION_SECRET` is required and must be a strong non-placeholder secret (32+ chars).
- `SUPERSET_OAUTH_DEFAULT_ROLE` defaults to least privilege (`Gamma`).
- Setting `SUPERSET_OAUTH_DEFAULT_ROLE=Admin` now also requires `SUPERSET_ALLOW_AUTO_ADMIN_ROLE=true`.

AKS default secret flow:
- `make k8s-aks-up` seeds Azure Key Vault from `.env`.
- AKS Secrets Store CSI sync keeps Kubernetes secret `odp-env` available for existing manifests.
- AKS manifests currently reference Kubernetes secret name `odp-env`; changing `AKS_KEY_VAULT_SECRET_NAME` is not supported.
- Set `AKS_USE_KEY_VAULT=false` to skip Key Vault sync and create `odp-env` directly from `.env`.

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
