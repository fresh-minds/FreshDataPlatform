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
