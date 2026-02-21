#!/usr/bin/env bash
# Shared library for kompose conversion and post-processing.
# Sourced by k8s_dev_up_full.sh (local Kind) and aks_up.sh (Azure AKS).
#
# Required variables before sourcing:
#   ROOT_DIR          – project root
#   KOMPOSE_OUT_DIR   – temp directory for generated manifests
#   KOMPOSE_OVERRIDE  – path to docker-compose.k8s.yml (or empty to skip)
#
# Optional variables:
#   SKIP_MSTEAMS      – "true" to remove prometheus-msteams manifests (arm64)

set -euo pipefail

K8S_SCRIPT_LOG_FORMAT="${K8S_SCRIPT_LOG_FORMAT:-text}"
K8S_SCRIPT_RUN_ID="${K8S_SCRIPT_RUN_ID:-$(date +%Y%m%dT%H%M%S)-$$}"
KOMPOSE_LOG_SOURCE="${KOMPOSE_LOG_SOURCE:-k8s-kompose-lib}"

_kompose_log_timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

_kompose_log_escape() {
  local value="${1:-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/ }"
  printf '%s' "$value"
}

kompose_log_event() {
  local level="$1"
  local event="$2"
  local outcome="$3"
  local message="${4:-}"
  local timestamp
  timestamp="$(_kompose_log_timestamp)"
  local escaped
  escaped="$(_kompose_log_escape "$message")"

  if [[ "$K8S_SCRIPT_LOG_FORMAT" == "text" || "$K8S_SCRIPT_LOG_FORMAT" == "both" ]]; then
    echo "[$KOMPOSE_LOG_SOURCE][$level][$event][$outcome] $message"
  fi

  if [[ "$K8S_SCRIPT_LOG_FORMAT" == "json" || "$K8S_SCRIPT_LOG_FORMAT" == "both" ]]; then
    printf '{"timestamp":"%s","level":"%s","source":"%s","run_id":"%s","event":"%s","outcome":"%s","namespace":"%s","message":"%s"}\n' \
      "$timestamp" "$level" "$KOMPOSE_LOG_SOURCE" "$K8S_SCRIPT_RUN_ID" "$event" "$outcome" "${NAMESPACE:-}" "$escaped"
  fi
}

_kompose_count_manifests() {
  local pattern="$1"
  find "$KOMPOSE_OUT_DIR" -maxdepth 1 -type f -name "$pattern" | wc -l | tr -d ' '
}

# ---------------------------------------------------------------------------
# kompose_generate – run kompose convert with main + override compose files
# ---------------------------------------------------------------------------
kompose_generate() {
  kompose_log_event "INFO" "kompose_generate" "start" "Running kompose convert into '$KOMPOSE_OUT_DIR'."
  local compose_args=(-f "$ROOT_DIR/docker-compose.yml")
  if [[ -n "${KOMPOSE_OVERRIDE:-}" && -f "$KOMPOSE_OVERRIDE" ]]; then
    compose_args+=(-f "$KOMPOSE_OVERRIDE")
  fi

  kompose convert \
    --volumes hostPath \
    "${compose_args[@]}" \
    -o "$KOMPOSE_OUT_DIR"

  local generated_count
  generated_count="$(_kompose_count_manifests '*.yaml')"
  kompose_log_event "INFO" "kompose_generate" "success" "Generated ${generated_count} manifests."
}

# ---------------------------------------------------------------------------
# kompose_remove_phase_a – remove manifests already managed by Phase A stack
# ---------------------------------------------------------------------------
kompose_remove_phase_a() {
  local before_count
  before_count="$(_kompose_count_manifests '*.yaml')"
  kompose_log_event "INFO" "kompose_remove_phase_a" "start" "Removing manifests already managed by Phase A stack."

  rm -f \
    "$KOMPOSE_OUT_DIR"/airflow-*.yaml \
    "$KOMPOSE_OUT_DIR"/create-buckets-*.yaml \
    "$KOMPOSE_OUT_DIR"/keycloak-*.yaml \
    "$KOMPOSE_OUT_DIR"/minio-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/minio-service.yaml \
    "$KOMPOSE_OUT_DIR"/postgres-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/postgres-service.yaml \
    "$KOMPOSE_OUT_DIR"/warehouse-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/warehouse-service.yaml \
    "$KOMPOSE_OUT_DIR"/datahub-*-setup-*.yaml \
    "$KOMPOSE_OUT_DIR"/datahub-upgrade-*.yaml \
    "$KOMPOSE_OUT_DIR"/airflow-init-*.yaml

  if [[ "${SKIP_MSTEAMS:-false}" == "true" ]]; then
    rm -f \
      "$KOMPOSE_OUT_DIR"/prometheus-msteams-deployment.yaml \
      "$KOMPOSE_OUT_DIR"/prometheus-msteams-service.yaml
  fi

  local after_count removed_count
  after_count="$(_kompose_count_manifests '*.yaml')"
  removed_count="$((before_count - after_count))"
  kompose_log_event "INFO" "kompose_remove_phase_a" "success" "Removed ${removed_count} manifests; ${after_count} remain."
}

# ---------------------------------------------------------------------------
# kompose_normalise_services – fix service port collisions
# ---------------------------------------------------------------------------
kompose_normalise_services() {
  kompose_log_event "INFO" "kompose_normalise_services" "start" "Normalizing generated Service ports."
  local manifest
  local changed_count=0
  for manifest in "$KOMPOSE_OUT_DIR"/*-service.yaml; do
    [[ -f "$manifest" ]] || continue
    yq -i '(.spec.ports[]? |= (.port = .targetPort))' "$manifest"
    changed_count=$((changed_count + 1))
  done

  # kompose emits both host-mapped and internal OTEL ports; after
  # normalization these collide on 4317/4318. Deduplicate explicitly.
  if [[ -f "$KOMPOSE_OUT_DIR/otel-collector-service.yaml" ]]; then
    yq -i '.spec.ports = [
      {"name":"otel-metrics","port":8889,"targetPort":8889,"protocol":"TCP"},
      {"name":"otlp-grpc","port":4317,"targetPort":4317,"protocol":"TCP"},
      {"name":"otlp-http","port":4318,"targetPort":4318,"protocol":"TCP"}
    ]' "$KOMPOSE_OUT_DIR/otel-collector-service.yaml"
  fi

  kompose_log_event "INFO" "kompose_normalise_services" "success" "Normalized ${changed_count} service manifests."
}

# ---------------------------------------------------------------------------
# kompose_fix_deployments – probe fixes, enableServiceLinks, superset args
# ---------------------------------------------------------------------------
kompose_fix_deployments() {
  kompose_log_event "INFO" "kompose_fix_deployments" "start" "Applying deployment probe and env-link safety fixes."
  local manifest
  local changed_count=0
  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue
    # Disable Kubernetes service-link env var injection to avoid
    # collisions like DATAHUB_GMS_PORT=tcp://... overriding app config.
    yq -i '.spec.template.spec.enableServiceLinks = false' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("livenessProbe") and .livenessProbe.exec.command and ((.livenessProbe.exec.command | length) == 1)) | .livenessProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("readinessProbe") and .readinessProbe.exec.command and ((.readinessProbe.exec.command | length) == 1)) | .readinessProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("startupProbe") and .startupProbe.exec.command and ((.startupProbe.exec.command | length) == 1)) | .startupProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    changed_count=$((changed_count + 1))
  done

  # Superset needs its bootstrap command preserved as a proper shell array.
  if [[ -f "$KOMPOSE_OUT_DIR/superset-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].args = [
      "sh",
      "-c",
      "pip install --no-cache-dir authlib && superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin || true && superset db upgrade && superset init && /usr/bin/run-server.sh & SERVER_PID=$! && echo [Superset] Waiting for /health... && for i in $(seq 1 60); do curl -sSf http://localhost:8088/health >/dev/null && break || sleep 2; done && python /app/scripts/superset/superset_bootstrap_job_market.py || true && wait $SERVER_PID"
    ]' "$KOMPOSE_OUT_DIR/superset-deployment.yaml"
  fi

  kompose_log_event "INFO" "kompose_fix_deployments" "success" "Updated ${changed_count} deployment manifests."
}

# ---------------------------------------------------------------------------
# kompose_hold_datahub – move GMS/frontend aside so they deploy after jobs
# ---------------------------------------------------------------------------
kompose_hold_datahub() {
  kompose_log_event "INFO" "kompose_hold_datahub" "start" "Holding DataHub manifests until setup jobs complete."
  local gms="$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
  local fe="$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml"
  [[ -f "$gms" ]] && mv "$gms" "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold"
  [[ -f "$fe" ]] && mv "$fe" "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold"
  kompose_log_event "INFO" "kompose_hold_datahub" "success" "DataHub hold manifests prepared."
}

# ---------------------------------------------------------------------------
# kompose_restore_datahub – bring held manifests back
# ---------------------------------------------------------------------------
kompose_restore_datahub() {
  kompose_log_event "INFO" "kompose_restore_datahub" "start" "Restoring held DataHub manifests."
  local gms="$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
  local fe="$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml"
  [[ -f "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold" ]] && mv "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold" "$gms"
  [[ -f "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold" ]] && mv "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold" "$fe"
  kompose_log_event "INFO" "kompose_restore_datahub" "success" "DataHub manifests restored."
}

# ---------------------------------------------------------------------------
# kompose_postprocess_local – Kind-specific volume rewrites
#   Required: KIND_MOUNT_PATH
# ---------------------------------------------------------------------------
kompose_postprocess_local() {
  kompose_log_event "INFO" "kompose_postprocess_local" "start" "Applying Kind-specific manifest rewrites."
  local manifest

  # Rewrite host paths from the actual repo directory to the Kind mount path
  while IFS= read -r -d '' manifest; do
    perl -0pi -e "s|\Q$ROOT_DIR\E|$KIND_MOUNT_PATH|g" "$manifest"
  done < <(find "$KOMPOSE_OUT_DIR" -type f -name '*.yaml' -print0)

  # Convert synthetic root hostPath volumes (project root) to emptyDir
  export KIND_MOUNT_PATH
  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue
    yq -i '(.spec.template.spec.volumes[]? | select(has("hostPath") and .hostPath.path == strenv(KIND_MOUNT_PATH))) |= {"name": .name, "emptyDir": {}}' "$manifest"
  done

  # Remove development bind mounts for images that run from container FS
  if [[ -f "$KOMPOSE_OUT_DIR/portal-deployment.yaml" ]]; then
    yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/portal-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml" ]]; then
    yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[]? | select(.name == "JUPYTER_WORKDIR").value) = "/workspace"' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
  fi

  kompose_log_event "INFO" "kompose_postprocess_local" "success" "Completed Kind-specific post-processing."
}

# ---------------------------------------------------------------------------
# kompose_postprocess_aks – AKS-specific: remove hostPaths, replace images
#   Required: ACR_NAME, PORTAL_API_IMAGE, MINIO_SSO_BRIDGE_IMAGE, JUPYTER_IMAGE
# ---------------------------------------------------------------------------
kompose_postprocess_aks() {
  kompose_log_event "INFO" "kompose_postprocess_aks" "start" "Applying AKS-specific manifest rewrites."
  local manifest

  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue

    # Replace all hostPath volumes with emptyDir (AKS has no host mounts).
    # Stateful services (databases, MinIO, etc.) get PVC via Phase A manifests.
    yq -i '(.spec.template.spec.volumes[]? | select(has("hostPath"))) |= {"name": .name, "emptyDir": {}}' "$manifest"
  done

  # Remove development bind mounts for images that run from container FS
  for svc in portal portal-api jupyter minio-sso-bridge superset; do
    if [[ -f "$KOMPOSE_OUT_DIR/${svc}-deployment.yaml" ]]; then
      yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/${svc}-deployment.yaml"
    fi
  done

  # Replace locally-built image references with ACR images
  if [[ -f "$KOMPOSE_OUT_DIR/portal-deployment.yaml" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${PORTAL_API_IMAGE:-}\"" "$KOMPOSE_OUT_DIR/portal-deployment.yaml" 2>/dev/null || true
    # portal uses the frontend image pushed by the Phase A part of aks_up.sh
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml" && -n "${PORTAL_API_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${PORTAL_API_IMAGE}\"" "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/minio-sso-bridge-deployment.yaml" && -n "${MINIO_SSO_BRIDGE_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${MINIO_SSO_BRIDGE_IMAGE}\"" "$KOMPOSE_OUT_DIR/minio-sso-bridge-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml" && -n "${JUPYTER_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${JUPYTER_IMAGE}\"" "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[]? | select(.name == "JUPYTER_WORKDIR").value) = "/workspace"' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
  fi

  kompose_log_event "INFO" "kompose_postprocess_aks" "success" "Completed AKS-specific post-processing."
}

# ---------------------------------------------------------------------------
# EXTENDED_DEPLOYMENTS – common list of deployments to wait for after apply
# ---------------------------------------------------------------------------
EXTENDED_DEPLOYMENTS=(
  minio-sso-bridge
  superset-db
  superset
  datahub-gms
  datahub-frontend
  portal
  portal-api
  jupyter
  alertmanager
  prometheus
  loki
  tempo
  otel-collector
  grafana
  statsd-exporter
  postgres-exporter-airflow
  postgres-exporter-warehouse
  promtail
)

DATAHUB_DEPS=(
  datahub-mysql
  datahub-elasticsearch
  datahub-zookeeper
  datahub-kafka
  datahub-schema-registry
)

DATAHUB_SETUP_JOBS=(
  datahub-mysql-setup
  datahub-elasticsearch-setup
  datahub-kafka-setup
  datahub-upgrade
)
