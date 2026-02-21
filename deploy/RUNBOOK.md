# Open Data Platform Kubernetes Runbook

This runbook deploys the production-hardened bundle in `/deploy` using Kustomize overlays.

## 1) Prerequisites

### Cluster add-ons
- Ingress controller installed (default assumed: `ingress-nginx`).
- `metrics-server` installed (required by HPA).
- A default `StorageClass` (or patch PVCs with your storage class).
- cert-manager (optional but recommended for TLS automation).
- External Secrets Operator (optional hardening path).

### Local tooling
- `kubectl` >= 1.28
- `kustomize` >= 5.0 (or `kubectl kustomize` support)
- `docker` with buildx enabled

### Required configuration before first deploy
- Render real values from `/deploy/base/secret-odp-env.template.yaml` OR create `odp-env` from your secret manager.
- Patch ingress hosts in the chosen overlay to your real DNS names.
- If cert-manager is enabled, set `cert-manager.io/cluster-issuer` to an issuer that exists in your cluster.

## 2) Build and Push Images

Set your registry and tag:

```bash
export REGISTRY="ghcr.io/<your-org>"
export IMAGE_TAG="$(git rev-parse --short HEAD)"

export AIRFLOW_IMAGE="${REGISTRY}/ai-trial/airflow:${IMAGE_TAG}"
export FRONTEND_IMAGE="${REGISTRY}/ai-trial/frontend:${IMAGE_TAG}"
```

Build and push Airflow image:

```bash
docker buildx build \
  --platform linux/amd64 \
  --file airflow/Dockerfile \
  --tag "${AIRFLOW_IMAGE}" \
  . \
  --push
```

Build and push frontend image:

```bash
docker buildx build \
  --platform linux/amd64 \
  --file frontend/Dockerfile.k8s \
  --tag "${FRONTEND_IMAGE}" \
  . \
  --push
```

## 3) Set Overlay Image References

Pick your target overlay (`dev`, `staging`, or `prod`) and set images:

```bash
export ENVIRONMENT="prod"
pushd "deploy/overlays/${ENVIRONMENT}"
kustomize edit set image ai-trial/airflow="${AIRFLOW_IMAGE}"
kustomize edit set image ai-trial/frontend="${FRONTEND_IMAGE}"
popd
```

## 4) Deploy

Create/update runtime secret first:

```bash
kubectl get namespace "odp-${ENVIRONMENT}" >/dev/null 2>&1 || kubectl create namespace "odp-${ENVIRONMENT}"

kubectl -n "odp-${ENVIRONMENT}" create secret generic odp-env \
  --from-env-file=.env \
  --dry-run=client -o yaml | kubectl apply -f -
```

Apply manifests:

```bash
kubectl apply -k "deploy/overlays/${ENVIRONMENT}"
```

Run bootstrap jobs when needed (first install or after secret reset):

```bash
kubectl -n "odp-${ENVIRONMENT}" delete job airflow-init --ignore-not-found
kubectl -n "odp-${ENVIRONMENT}" delete job minio-create-buckets --ignore-not-found

kubectl apply -k "deploy/overlays/${ENVIRONMENT}"
```

Wait for workloads:

```bash
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/postgres --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/postgres --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/warehouse --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/minio --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/airflow-webserver --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/airflow-scheduler --timeout=10m
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/frontend --timeout=10m
```

## 5) Verify Checklist

### Control plane health

```bash
kubectl -n "odp-${ENVIRONMENT}" get pods
kubectl -n "odp-${ENVIRONMENT}" get deploy,hpa,pdb,ingress,pvc
kubectl -n "odp-${ENVIRONMENT}" get events --sort-by=.lastTimestamp | tail -n 50
```

Expected:
- Pods `Running`/`Completed` (jobs).
- Deployment rollouts complete and PVCs are `Bound`.
- HPA objects present and not reporting metric API errors.

### Probe validation

```bash
kubectl -n "odp-${ENVIRONMENT}" describe pod -l app.kubernetes.io/name=airflow-webserver
kubectl -n "odp-${ENVIRONMENT}" describe pod -l app.kubernetes.io/name=frontend
kubectl -n "odp-${ENVIRONMENT}" describe pod -l app.kubernetes.io/name=minio
```

Expected:
- `Readiness probe succeeded`
- `Liveness probe succeeded`
- No repeated `Back-off restarting failed container` events.

### App path validation
- Browse to `https://app.<your-domain>`.
- Browse to `https://airflow.<your-domain>` and verify login works.
- Browse to `https://minio.<your-domain>` and verify console availability.

### Scaling checks

```bash
kubectl -n "odp-${ENVIRONMENT}" scale deployment frontend --replicas=4
kubectl -n "odp-${ENVIRONMENT}" get pods -l app.kubernetes.io/name=frontend -w
```

Expected:
- New replicas schedule and become `Ready`.
- Service traffic remains available during rollout.

## 6) Rollback

### Fast rollback (deployment image/config)

```bash
kubectl -n "odp-${ENVIRONMENT}" rollout undo deployment/frontend
kubectl -n "odp-${ENVIRONMENT}" rollout undo deployment/airflow-webserver
kubectl -n "odp-${ENVIRONMENT}" rollout undo deployment/airflow-scheduler
```

Verify:

```bash
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/frontend
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/airflow-webserver
kubectl -n "odp-${ENVIRONMENT}" rollout status deployment/airflow-scheduler
```

### Data rollback
- Stateful services (`postgres`, `warehouse`, `minio`) require storage-level snapshots/backups.
- Restore PVC data from backup tooling before redeploying if a data regression occurs.

## 7) Troubleshooting

### Common commands

```bash
kubectl -n "odp-${ENVIRONMENT}" get pods -o wide
kubectl -n "odp-${ENVIRONMENT}" describe pod <pod-name>
kubectl -n "odp-${ENVIRONMENT}" logs <pod-name> --all-containers --tail=200
kubectl -n "odp-${ENVIRONMENT}" top pod
kubectl -n "odp-${ENVIRONMENT}" get networkpolicy
```

### What to look for
- `ImagePullBackOff`: registry auth or image tag mismatch.
- `CrashLoopBackOff`: missing/invalid secret values, or read-only filesystem incompatibility.
- `Readiness probe failed`: wrong endpoint path, app not fully initialized, or dependency unavailable.
- `FailedScheduling`: insufficient cluster CPU/memory or PVC provisioning issues.
- Network timeouts: validate `NetworkPolicy` allows DNS and required egress.

### Airflow specific

```bash
kubectl -n "odp-${ENVIRONMENT}" logs deploy/airflow-webserver --tail=200
kubectl -n "odp-${ENVIRONMENT}" logs deploy/airflow-scheduler --tail=200
kubectl -n "odp-${ENVIRONMENT}" logs job/airflow-init --tail=200
```

Focus on:
- DB connection string errors.
- Fernet key format errors.
- Migration/user bootstrap failures.

### MinIO specific

```bash
kubectl -n "odp-${ENVIRONMENT}" logs deploy/minio --tail=200
kubectl -n "odp-${ENVIRONMENT}" logs job/minio-create-buckets --tail=200
```

Focus on:
- Invalid root credentials.
- Bucket bootstrap failures.
- PVC mount or permission issues.

## 8) Observability and Golden Signals

Track at minimum:
- Latency: ingress and app response time (`p95`, `p99`).
- Traffic: request rate per service and ingress backend.
- Errors: 4xx/5xx rates, failed jobs, probe failures.
- Saturation: CPU/memory usage, pod restarts, queue lag (scheduler backlog).

Recommended follow-up hardening:
- Emit JSON structured logs for all custom components.
- Add `ServiceMonitor`/`PodMonitor` CRDs if Prometheus Operator is available.
- Enable SBOM generation and image signing in CI.
