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

AKS modular helpers:
- `scripts/aks/aks_up_lib.sh`: shared helper functions used by `scripts/aks/aks_up.sh`
	(retryable rollout waits, diagnostics, namespaced apply helper, and image build/push helper).
- `scripts/aks/aks_update_images.sh`: minimal AKS image-only updater (build/push selected images, patch existing deployments, wait rollout).

## Conventions for new scripts

- Put domain-specific scripts in the matching subfolder.
- If relocating an existing script, update Makefile/CI/docs references in the same change.

## Security-sensitive script behavior

### Bootstrap env auto-fill

`./scripts/platform/bootstrap_all.sh --auto-fill-env` now auto-generates missing or placeholder values for:

- `KEYCLOAK_GATEWAY_CLIENT_SECRET`
- `MINIO_SSO_BRIDGE_SESSION_SECRET`

Verification:

```bash
grep -E '^(KEYCLOAK_GATEWAY_CLIENT_SECRET|MINIO_SSO_BRIDGE_SESSION_SECRET)=' .env
```

### kind shared SSO gateway setup

`./scripts/k8s/k8s_enable_sso_gateway.sh` now fails fast when `KEYCLOAK_GATEWAY_CLIENT_SECRET` is missing or still a placeholder (`change_me*`).

Verification before running:

```bash
grep '^KEYCLOAK_GATEWAY_CLIENT_SECRET=' .env
kubectl -n odp-dev get secret odp-env -o jsonpath='{.data.KEYCLOAK_GATEWAY_CLIENT_SECRET}' | base64 --decode; echo
```

### AKS DataHub MySQL self-heal safety

`./scripts/aks/aks_up.sh` self-heal paths for DataHub MySQL host auth / missing schema now:

- grant the DataHub app user from secret keys (`DATAHUB_MYSQL_USER`, `DATAHUB_MYSQL_PASSWORD`)
- scope privileges to `DATAHUB_MYSQL_DATABASE`
- avoid creating or altering remote `root@'%'` grants

Verify required secret keys before AKS deploy:

```bash
grep -E '^(DATAHUB_MYSQL_USER|DATAHUB_MYSQL_PASSWORD|DATAHUB_MYSQL_DATABASE)=' .env
```

## dbt docs auto-regeneration watcher

For dbt lineage/docs development, run:

```bash
make dbt-docs-watch
```

This starts required services and runs `scripts/platform/watch_dbt_docs.py`,
which regenerates `dbt_parallel/target/` docs artifacts automatically whenever
dbt project files change.

## Kubernetes script logging

`scripts/k8s/k8s_dev_up_full.sh` supports structured logging controls for observability pipelines:

- `K8S_SCRIPT_LOG_FORMAT=text|json|both` (default: `text`)
- `K8S_SCRIPT_RUN_ID=<correlation-id>` to tie all events to a single run

Example:

```bash
K8S_SCRIPT_LOG_FORMAT=json K8S_SCRIPT_RUN_ID=local-kind-rollout make k8s-dev-up-full
```

## Observability verification script

Use the Compose verification script to validate end-to-end ingestion into Loki/Prometheus/Grafana:

```bash
./scripts/testing/verify_compose_observability.sh
```

Optional lookback window (seconds):

```bash
OBS_LOOKBACK_SECONDS=1800 ./scripts/testing/verify_compose_observability.sh
```

Optional strict trace-volume mode:

```bash
OBS_REQUIRE_TRACE_VOLUME=true OBS_TRACE_VOLUME_WINDOW_SECONDS=30 OBS_TRACE_VOLUME_MIN_SPANS=10 ./scripts/testing/verify_compose_observability.sh
```

Ambient strict mode (no synthetic probes):

```bash
OBS_REQUIRE_TRACE_VOLUME=true OBS_TRACE_VOLUME_MODE=ambient OBS_TRACE_VOLUME_WINDOW_SECONDS=60 OBS_TRACE_VOLUME_MIN_SPANS=5 ./scripts/testing/verify_compose_observability.sh
```

When `OBS_TRACE_VOLUME_MODE=ambient`, the script skips synthetic trace injection and checks only naturally occurring trace volume.

The script also injects a synthetic OTLP trace into the collector and verifies the trace is retrievable from Tempo's query API.

## AKS smoke verification script

Use the AKS smoke script to validate in-cluster HTTP health for observability and core platform services (Airflow, DataHub, MinIO, Superset, Jupyter, and related endpoints):

```bash
./scripts/testing/verify_aks_smoke.sh
```

Run via Make:

```bash
make k8s-aks-smoke
```

The script retries each HTTP endpoint check for roughly 60 seconds before marking it RED to absorb short post-rollout warm-up windows.

Optional overrides:

```bash
NAMESPACE=odp-dev AKS_SMOKE_WAIT_TIMEOUT_SECONDS=180 make k8s-aks-smoke
```

## AKS provisioning script

Use the full AKS provisioning/deploy flow:

```bash
make k8s-aks-up
```

Defaults:
- Secrets are synced from `.env` to Azure Key Vault and then projected into Kubernetes secret `odp-env`.
- Post-deploy `make k8s-aks-smoke` runs automatically when `AKS_SMOKE_AFTER_UP` is unset/empty (or explicitly `true`); set `AKS_SMOKE_AFTER_UP=false` to skip.
- Re-running `make k8s-aks-up` is safe when the AKS Key Vault provider add-on is already enabled; the script detects this and skips re-enabling it.
- When Key Vault RBAC is enabled, the script attempts to grant the signed-in principal `Key Vault Secrets Officer` if missing (requires role-assignment permission at Key Vault scope).
- Key Vault secret writes retry on RBAC propagation delays (`AKS_KEY_VAULT_SECRET_SET_RETRIES`, `AKS_KEY_VAULT_SECRET_SET_RETRY_DELAY_SECONDS`).
- Empty `.env` values are skipped during Key Vault sync because Azure Key Vault does not allow empty secret values.
- AKS `.env` parsing strips one wrapping quote pair before Key Vault sync (for example `'tenant-id'` -> `tenant-id`) to avoid quoted credential regressions.
- `AKS_NODE_COUNT` is enforced as minimum System nodepool capacity on reruns (direct scale for fixed nodepools; autoscaler min-count update for autoscaled nodepools).

Key Vault override example:

```bash
AKS_KEY_VAULT_NAME=aitrialkv1234abcd AKS_KEY_VAULT_RESOURCE_GROUP=ai-trial-rg make k8s-aks-up
```

Fallback to direct `.env` -> Kubernetes secret:

```bash
AKS_USE_KEY_VAULT=false make k8s-aks-up
```

Constraint:
- AKS manifests in this repository currently reference Kubernetes secret name `odp-env`; overriding `AKS_KEY_VAULT_SECRET_NAME` is not supported.

## Troubleshooting AKS postprocess hook mismatch

Summary:
- AKS rollout now supports both postprocess hook names: `kompose_postprocess_aks` (current) and `se_postprocess_aks` (legacy alias) to avoid `command not found` failures on mixed script revisions.

Prerequisites:
- Run from the repository root.
- Ensure local changes include the updated `scripts/k8s/k8s_kompose_lib.sh`.

Command:

```bash
make k8s-aks-up
```

Verification:

```bash
bash -n scripts/aks/aks_up.sh
bash -n scripts/k8s/k8s_kompose_lib.sh
make k8s-aks-up
```

Constraint:
- This compatibility alias only covers the hook-name mismatch; other AKS rollout failures still require normal diagnostics from `make k8s-aks-up` output.

## AKS image-only update script

Use this to iterate faster after an initial `make k8s-aks-up`:

```bash
make k8s-aks-update-images
```

When `AKS_IMAGES` includes `airflow`, the script also refreshes `dbt-docs`
by patching deployment `dbt-docs` initContainer image to the same Airflow
image and bumping annotation `dbt-docs/build-id`.
Optional override: set `DBT_DOCS_BUILD_ID=<custom-id>` to control that
annotation value.

Update only selected services:

```bash
AKS_IMAGES=frontend,portal-api AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT=900s make k8s-aks-update-images
```

Supported `AKS_IMAGES` values: `airflow`, `frontend`, `portal-api`, `jupyter`, `minio-sso-bridge`.
