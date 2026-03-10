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
- `scripts/testing/verify_compose_minimal.sh`: smoke checks for the bare-minimum Docker Compose stack (`docker-compose.minimal.yml`).
- `scripts/k8s/`: kind/Kubernetes helper scripts.
- `scripts/aks/`: AKS provisioning/teardown scripts.
- `scripts/aks/scaleway_redeploy_all.sh`: one-command Scaleway redeploy helper (Terraform + workloads + smoke checks).
- `scripts/aks/scaleway_destroy_all.sh`: dedicated Terraform-backed Scaleway teardown helper.

AKS modular helpers:
- `scripts/aks/aks_up_lib.sh`: shared helper functions used by `scripts/aks/aks_up.sh`
	(retryable rollout waits, diagnostics, namespaced apply helper, and image build/push helper).
- `scripts/aks/aks_update_images.sh`: minimal AKS image-only updater (build/push selected images, patch existing deployments, wait rollout; refreshes Airflow webserver ConfigMap when `AKS_IMAGES` includes `airflow`).

## Scaleway teardown script

Summary:
- `scripts/aks/scaleway_destroy_all.sh` destroys all resources managed by `terraform/scaleway`.
- It supports dry-run mode for safe review before deletion.
- Optional `--purge-leftovers` also removes leftover Registry namespaces and LB IPs in the same Scaleway project.

Prerequisites:
- `terraform` installed.
- `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`, and `SCW_DEFAULT_PROJECT_ID` exported (for example via `.env`).
- Existing Terraform state in `terraform/scaleway/terraform.tfstate` for the environment you want to destroy.

Dry-run (plan-only):

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_destroy_all.sh --dry-run --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Dry-run with leftover purge preview:

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_destroy_all.sh --dry-run --purge-leftovers --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Destroy:

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_destroy_all.sh --yes --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Destroy + purge leftovers:

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_destroy_all.sh --yes --purge-leftovers --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Make target wrappers:

```bash
DRY_RUN=true make scaleway-destroy-all
make scaleway-destroy-all
PURGE_LEFTOVERS=true make scaleway-destroy-all
```

Verification:

```bash
terraform -chdir=terraform/scaleway state list
```

Expected result:
- No resources are listed in Terraform state.

## Scaleway redeploy script

Summary:
- `scripts/aks/scaleway_redeploy_all.sh` runs a full Scaleway redeploy flow in one command.
- Default flow: Terraform apply in `terraform/scaleway`, then workload deployment via `scripts/aks/aks_up.sh`, then `scripts/testing/verify_aks_smoke.sh`.
- Supports partial runs (`--skip-terraform-apply`, `--skip-deploy`, `--skip-smoke`, `--skip-image-build`) and dry-run Terraform plan (`--dry-run`).
- Before deployment, it runs a registry preflight push check to fail fast when the active `SCW_SECRET_KEY` lacks push rights (`--skip-registry-preflight` to disable).
- For Scaleway pushes, the flow defaults to classic Docker push plus legacy Docker builder (`AKS_USE_LEGACY_DOCKER_BUILDER=true`) to avoid intermittent BuildKit layer push stalls (`insufficient_scope` + prolonged `Waiting` states).
- The build platform defaults to `linux/amd64` and can be overridden via `AKS_DOCKER_BUILD_PLATFORM`.
- If legacy builder fails with a platform mismatch (`does not provide the specified platform`), the script automatically retries that image build using `docker buildx build --load` for the requested platform.
- If Terraform apply hits Scaleway IAM 409 conflicts for pre-existing secrets-reader resources, the script attempts one automatic import+retry.
- If Terraform apply hits a duplicate default pool-name conflict, the script attempts one automatic pool import+retry.
- If Terraform apply hits a transient Kubernetes API timeout while creating Helm releases, the script retries apply automatically (`SCW_TERRAFORM_APPLY_RETRIES`, default `4`; `SCW_TERRAFORM_APPLY_RETRY_DELAY_SECONDS`, default `20`).
- If `scw` CLI is unavailable but `KUBE_CONFIG_COMMAND` references `scw`, the script falls back to the current kubeconfig context.

Prerequisites:
- `terraform`, `kubectl`, `docker`, `jq`, `yq`, `kompose`, `curl`, and `openssl` installed.
- `SCW_ACCESS_KEY`, `SCW_SECRET_KEY`, and `SCW_DEFAULT_PROJECT_ID` exported (for example via `.env`).
- `terraform/environments/scaleway-dev.tfvars` present (or provide `--tf-vars-file`).

Constraints:
- The active Scaleway key must have Container Registry push permissions for the target registry namespace, otherwise image push fails with `insufficient_scope`.

Dry-run (plan-only):

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_redeploy_all.sh --dry-run --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Full redeploy:

```bash
set -a && source .env && set +a
./scripts/aks/scaleway_redeploy_all.sh --yes --tf-vars-file terraform/environments/scaleway-dev.tfvars
```

Partial examples:

```bash
./scripts/aks/scaleway_redeploy_all.sh --yes --skip-smoke
./scripts/aks/scaleway_redeploy_all.sh --yes --skip-terraform-apply
./scripts/aks/scaleway_redeploy_all.sh --yes --skip-terraform-apply --skip-image-build --minimal
./scripts/aks/scaleway_redeploy_all.sh --yes --skip-registry-preflight
```

Make target wrapper:

```bash
DRY_RUN=true make scaleway-redeploy-all
make scaleway-redeploy-all
SKIP_SMOKE=true make scaleway-redeploy-all
SKIP_TERRAFORM_APPLY=true SKIP_IMAGE_BUILD=true make scaleway-redeploy-all-minimal
```

## Conventions for new scripts

- Put domain-specific scripts in the matching subfolder.
- If relocating an existing script, update Makefile/CI/docs references in the same change.

## ODP Staffing Demand entrypoints (Phase 1)

Summary:
- Canonical pipeline entrypoints are now exposed under `odp_staffing_demand` names.
- Legacy `job_market` entrypoints remain as compatibility aliases during migration.

Canonical scripts:
- `scripts/pipeline/run_odp_staffing_demand_pipeline.py`
- `scripts/pipeline/run_odp_staffing_demand_metadata_pipeline.py`
- `scripts/superset/superset_bootstrap_odp_staffing_demand.py`

Verification:
```bash
make run-odp-staffing-demand
make run-odp-staffing-demand-metadata
```

Phase 2 additions:
- Canonical import path package added: `pipelines/odp_staffing_demand/`.
- `scripts/pipeline/run_local.py` now accepts canonical pipeline IDs like:
  - `odp_staffing_demand.bronze_adzuna_jobs`
- Legacy `odp_staffing_demand.*` pipeline IDs remain available as compatibility aliases.

## Superset metadata dashboard bootstrap

Summary:
- `scripts/superset/superset_bootstrap_platform_metadata.py` builds Superset datasets/charts/dashboard
  from warehouse operational metadata in schema `platform_metadata`.
- `scripts/platform/bootstrap_all.sh` runs this script automatically during the Superset bootstrap step.

Prerequisites:
- Superset container is running (`open-data-platform-superset`).
- Warehouse contains initialized `platform_metadata` tables (for example via
  `scripts/warehouse/init_platform_metadata.py` or `make bootstrap-all`).

Run manually:

```bash
docker exec open-data-platform-superset python /app/scripts/superset/superset_bootstrap_platform_metadata.py
```

Verification:

```bash
docker exec open-data-platform-superset sh -lc 'python - <<"PY"
import sqlite3
conn = sqlite3.connect("/app/superset_home/superset.db")
cur = conn.cursor()
cur.execute("select dashboard_title from dashboards order by dashboard_title")
print([row[0] for row in cur.fetchall()])
PY'
```

Expected dashboard title includes:
- `Platform Metadata Operations`

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

### Runtime license risk triage

Use `./scripts/quality/check_license_risk.sh` (or `make license-risk-check`) to flag potentially restrictive image licenses in `docker-compose.yml` (AGPL/GPL/source-available families) for manual legal review.

Use `FAIL_ON_RESTRICTIVE=true` to fail automated checks when high-risk image families are present.

## dbt docs auto-regeneration watcher

For dbt lineage/docs development, run:

```bash
make dbt-docs-watch
```

This starts required services and runs `scripts/platform/watch_dbt_docs.py`,
which regenerates `dbt/target/` docs artifacts automatically whenever
dbt project files change.

## dbt bootstrap runner threading

`scripts/pipeline/run_dbt.sh` now runs dbt commands with
`DBT_THREADS=1` by default to avoid Postgres DDL deadlocks during bootstrap.

Override when needed:

```bash
DBT_THREADS=4 scripts/pipeline/run_dbt.sh
```

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

When `AKS_IMAGES` includes `airflow`, the script also:
- refreshes ConfigMap `airflow-webserver-config` from
  `airflow/webserver_config.py` before the Airflow deployment rollout, and
- refreshes `dbt-docs` by patching deployment `dbt-docs` initContainer image
  to the same Airflow image and bumping annotation `dbt-docs/build-id`.
  The initContainer update preserves existing command/env/volume fields.
Optional override: set `DBT_DOCS_BUILD_ID=<custom-id>` to control that
annotation value.

Update only selected services:

```bash
AKS_IMAGES=frontend,portal-api AKS_IMAGE_UPDATE_ROLLOUT_TIMEOUT=900s make k8s-aks-update-images
```

Supported `AKS_IMAGES` values: `airflow`, `frontend`, `portal-api`, `jupyter`, `minio-sso-bridge`.
