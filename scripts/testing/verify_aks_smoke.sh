#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-odp-dev}"
SMOKE_IMAGE="${AKS_SMOKE_IMAGE:-curlimages/curl:8.10.1}"
WAIT_TIMEOUT_SECONDS="${AKS_SMOKE_WAIT_TIMEOUT_SECONDS:-120}"
POD_NAME="aks-smoke-$(date +%s)"

PASS_COUNT=0
FAIL_COUNT=0
SKIP_COUNT=0

log() {
  echo "[verify-aks-smoke] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

record_result() {
  local status="$1"
  local check_name="$2"
  local detail="$3"

  case "$status" in
    GREEN) PASS_COUNT=$((PASS_COUNT + 1)) ;;
    RED) FAIL_COUNT=$((FAIL_COUNT + 1)) ;;
    SKIP) SKIP_COUNT=$((SKIP_COUNT + 1)) ;;
  esac

  printf '%-5s | %-30s | %s\n' "$status" "$check_name" "$detail"
}

service_exists() {
  local svc_name="$1"
  kubectl -n "$NAMESPACE" get svc "$svc_name" >/dev/null 2>&1
}

check_http() {
  local check_name="$1"
  local svc_name="$2"
  local url="$3"
  local expected_codes="$4"
  local code=""
  local attempt=1
  local max_attempts=12
  local retry_delay_seconds=5

  if ! service_exists "$svc_name"; then
    record_result "SKIP" "$check_name" "service '$svc_name' not found"
    return 0
  fi

  while (( attempt <= max_attempts )); do
    code="$(kubectl -n "$NAMESPACE" exec "$POD_NAME" -- sh -lc "curl -sS -o /dev/null -w '%{http_code}' '$url' || true")"

    if [[ ",$expected_codes," == *",$code,"* ]]; then
      if (( attempt == 1 )); then
        record_result "GREEN" "$check_name" "code=$code url=$url"
      else
        record_result "GREEN" "$check_name" "code=$code url=$url retries=$((attempt - 1))"
      fi
      return 0
    fi

    if (( attempt < max_attempts )); then
      sleep "$retry_delay_seconds"
    fi
    attempt=$((attempt + 1))
  done

  record_result "RED" "$check_name" "code=$code expected=$expected_codes url=$url attempts=$max_attempts"
}

check_prometheus_targets() {
  if ! service_exists "prometheus"; then
    record_result "SKIP" "Prometheus targets API" "service 'prometheus' not found"
    return 0
  fi

  local payload
  payload="$(kubectl -n "$NAMESPACE" exec "$POD_NAME" -- sh -lc "curl -sS 'http://prometheus:9090/api/v1/targets' || true")"

  if echo "$payload" | grep -q '"status":"success"'; then
    local up_count down_count
    up_count="$(echo "$payload" | grep -o '"health":"up"' | wc -l | tr -d ' ' || true)"
    down_count="$(echo "$payload" | grep -o '"health":"down"' | wc -l | tr -d ' ' || true)"
    record_result "GREEN" "Prometheus targets API" "up=$up_count down=$down_count"
  else
    record_result "RED" "Prometheus targets API" "query failed"
  fi
}

check_loki_query_api() {
  if ! service_exists "loki"; then
    record_result "SKIP" "Loki query API" "service 'loki' not found"
    return 0
  fi

  local payload
  payload="$(kubectl -n "$NAMESPACE" exec "$POD_NAME" -- sh -lc "curl -sS 'http://loki:3100/loki/api/v1/labels' || true")"

  if echo "$payload" | grep -q '"status":"success"'; then
    record_result "GREEN" "Loki query API" "labels endpoint"
  else
    record_result "RED" "Loki query API" "labels endpoint failed"
  fi
}

cleanup() {
  kubectl -n "$NAMESPACE" delete pod "$POD_NAME" --ignore-not-found=true --wait=false >/dev/null 2>&1 || true
}

main() {
  require_cmd kubectl

  trap cleanup EXIT

  log "Creating temporary smoke-check pod in namespace '$NAMESPACE'..."
  kubectl -n "$NAMESPACE" run "$POD_NAME" \
    --image "$SMOKE_IMAGE" \
    --restart Never \
    --command -- sleep 900 >/dev/null

  kubectl -n "$NAMESPACE" wait --for=condition=Ready "pod/$POD_NAME" --timeout="${WAIT_TIMEOUT_SECONDS}s" >/dev/null

  echo "STATUS | CHECK                          | DETAIL"
  echo "-------+--------------------------------+-----------------------------------------------"

  log "Running observability checks..."
  check_http "Grafana root" "grafana" "http://grafana:3000/" "200,302"
  check_http "Prometheus ready" "prometheus" "http://prometheus:9090/-/ready" "200"
  check_http "Loki ready" "loki" "http://loki:3100/ready" "200"
  check_http "Tempo ready" "tempo" "http://tempo:3200/ready" "200"
  check_http "OTel metrics" "otel-collector" "http://otel-collector:8889/metrics" "200"
  check_http "Alertmanager healthy" "alertmanager" "http://alertmanager:9093/-/healthy" "200"
  check_prometheus_targets
  check_loki_query_api

  log "Running core platform checks..."
  check_http "Airflow health" "airflow-webserver" "http://airflow-webserver:8080/health" "200"
  check_http "DataHub frontend" "datahub-frontend" "http://datahub-frontend:9002/" "200,302"
  check_http "DataHub GMS health" "datahub-gms" "http://datahub-gms:8080/health" "200"
  check_http "Schema Registry subjects" "datahub-schema-registry" "http://datahub-schema-registry:8081/subjects" "200"
  check_http "MinIO live" "minio" "http://minio:9000/minio/health/live" "200"
  check_http "Superset health" "superset" "http://superset:8088/health" "200"
  check_http "Jupyter API" "jupyter" "http://jupyter:8888/api" "200,302"
  check_http "DataHub Elasticsearch" "datahub-elasticsearch" "http://datahub-elasticsearch:9200/_cluster/health" "200"

  echo
  echo "Summary: GREEN=$PASS_COUNT RED=$FAIL_COUNT SKIP=$SKIP_COUNT"

  if (( FAIL_COUNT > 0 )); then
    log "Smoke test failed. Investigate RED checks above."
    exit 1
  fi

  log "All smoke checks passed."
}

main "$@"
