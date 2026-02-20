#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-odp-dev}"
STATE_DIR="${STATE_DIR:-/tmp/ai_trial_k8s_port_forward}"
PID_DIR="$STATE_DIR/pids"
LOG_DIR="$STATE_DIR/logs"

mkdir -p "$PID_DIR" "$LOG_DIR"

MAPPINGS=(
  "airflow-webserver|8080|svc/airflow-webserver|8080|tcp|required"
  "keycloak|8090|svc/keycloak|8090|tcp|required"
  "minio-api|9000|svc/minio|9000|tcp|required"
  "minio-console|9001|svc/minio|9001|tcp|required"
  "minio-sso-bridge|9011|svc/minio-sso-bridge|9011|tcp|required"
  "warehouse|5433|svc/warehouse|5432|tcp|required"
  "superset|8088|svc/superset|8088|tcp|required"
  "datahub-elasticsearch|9200|svc/datahub-elasticsearch|9200|tcp|required"
  "datahub-kafka|9092|svc/datahub-kafka|9092|tcp|required"
  "datahub-gms|8081|svc/datahub-gms|8080|tcp|required"
  "datahub-frontend|9002|svc/datahub-frontend|9002|tcp|required"
  "portal|3000|svc/portal|3000|tcp|required"
  "jupyter|8888|svc/jupyter|8888|tcp|required"
  "prometheus|9090|svc/prometheus|9090|tcp|required"
  "alertmanager|9093|svc/alertmanager|9093|tcp|required"
  "prometheus-msteams|2000|svc/prometheus-msteams|2000|tcp|optional"
  "grafana|3001|svc/grafana|3000|tcp|required"
  "loki|3100|svc/loki|3100|tcp|required"
  "tempo-ui|3200|svc/tempo|3200|tcp|required"
  "tempo-otlp-grpc|4317|svc/tempo|4317|tcp|required"
  "tempo-otlp-http|4318|svc/tempo|4318|tcp|required"
  "otel-metrics|8889|svc/otel-collector|8889|tcp|required"
  "otel-otlp-grpc|4319|svc/otel-collector|4317|tcp|required"
  "otel-otlp-http|4320|svc/otel-collector|4318|tcp|required"
  "statsd-exporter|9102|svc/statsd-exporter|9102|tcp|required"
  "statsd-udp|9125|svc/statsd-exporter|9125|udp|unsupported"
  "postgres-exporter-airflow|9187|svc/postgres-exporter-airflow|9187|tcp|required"
  "postgres-exporter-warehouse|9188|svc/postgres-exporter-warehouse|9187|tcp|required"
)

log() {
  echo "[k8s-port-forward] $*"
}

pid_file() {
  local name="$1"
  echo "$PID_DIR/${name}.pid"
}

log_file() {
  local name="$1"
  echo "$LOG_DIR/${name}.log"
}

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

service_name_from_resource() {
  local resource="$1"
  echo "${resource#svc/}"
}

has_endpoints() {
  local service_name="$1"
  local count
  count="$(kubectl -n "$NAMESPACE" get endpoints "$service_name" -o jsonpath='{.subsets[*].addresses[*].ip}' 2>/dev/null | wc -w | tr -d ' ')"
  [[ "$count" -gt 0 ]]
}

ensure_service_exists() {
  local resource="$1"
  kubectl -n "$NAMESPACE" get "$resource" >/dev/null 2>&1
}

kill_listener_if_ours() {
  local local_port="$1"
  local listener_pids
  listener_pids="$(lsof -t -nP -iTCP:"$local_port" -sTCP:LISTEN 2>/dev/null || true)"
  if [[ -z "$listener_pids" ]]; then
    return 0
  fi

  while IFS= read -r pid; do
    [[ -z "$pid" ]] && continue
    local cmd
    cmd="$(ps -o command= -p "$pid" 2>/dev/null || true)"
    if [[ "$cmd" == *"kubectl -n $NAMESPACE port-forward"* ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      sleep 0.2
    fi
  done <<< "$listener_pids"
}

start_one() {
  local name="$1" local_port="$2" resource="$3" remote_port="$4" proto="$5" mode="$6"
  local pf pidf logf service_name

  if [[ "$proto" == "udp" ]]; then
    log "SKIP $name ($local_port/$proto): kubectl port-forward only supports TCP"
    return 0
  fi

  if ! ensure_service_exists "$resource"; then
    if [[ "$mode" == "optional" || "$mode" == "unsupported" ]]; then
      log "SKIP $name ($local_port): service $resource not present"
      return 0
    fi
    log "WARN $name ($local_port): service $resource not found"
    return 1
  fi

  service_name="$(service_name_from_resource "$resource")"
  if [[ "$mode" == "optional" ]] && ! has_endpoints "$service_name"; then
    log "SKIP $name ($local_port): service has no ready endpoints"
    return 0
  fi

  pidf="$(pid_file "$name")"
  logf="$(log_file "$name")"

  if [[ -f "$pidf" ]]; then
    pf="$(cat "$pidf" 2>/dev/null || true)"
    if [[ -n "$pf" ]] && is_pid_alive "$pf"; then
      log "OK $name already forwarding localhost:$local_port -> $resource:$remote_port"
      return 0
    fi
    rm -f "$pidf"
  fi

  kill_listener_if_ours "$local_port"

  if lsof -nP -iTCP:"$local_port" -sTCP:LISTEN >/dev/null 2>&1; then
    log "WARN $name ($local_port): local port already in use by another process"
    return 1
  fi

  nohup bash -lc "
    while true; do
      kubectl -n '$NAMESPACE' port-forward '$resource' '${local_port}:${remote_port}' >>'$logf' 2>&1 || true
      sleep 1
    done
  " > /dev/null 2>&1 < /dev/null &
  pf=$!
  echo "$pf" >"$pidf"

  sleep 0.4
  if is_pid_alive "$pf"; then
    log "STARTED $name localhost:$local_port -> $resource:$remote_port (pid $pf)"
    return 0
  fi

  log "WARN $name failed to start (see $logf)"
  rm -f "$pidf"
  return 1
}

stop_all() {
  local any=false
  for mapping in "${MAPPINGS[@]}"; do
    IFS='|' read -r name _ _ _ _ _ <<< "$mapping"
    local pf pidf
    pidf="$(pid_file "$name")"
    [[ -f "$pidf" ]] || continue
    pf="$(cat "$pidf" 2>/dev/null || true)"
    if [[ -n "$pf" ]] && is_pid_alive "$pf"; then
      kill "$pf" >/dev/null 2>&1 || true
      any=true
      log "STOPPED $name (pid $pf)"
    fi
    rm -f "$pidf"
  done

  if [[ "$any" == false ]]; then
    log "No tracked port-forward processes were running"
  fi
}

status_all() {
  for mapping in "${MAPPINGS[@]}"; do
    IFS='|' read -r name local_port resource remote_port proto mode <<< "$mapping"
    local pidf pf status
    pidf="$(pid_file "$name")"
    status="stopped"
    if [[ -f "$pidf" ]]; then
      pf="$(cat "$pidf" 2>/dev/null || true)"
      if [[ -n "$pf" ]] && is_pid_alive "$pf"; then
        status="running (pid $pf)"
      fi
    fi
    echo "$name\t$proto\tlocalhost:$local_port -> $resource:$remote_port\t$status\t$mode"
  done
}

start_all() {
  local failures=0
  for mapping in "${MAPPINGS[@]}"; do
    IFS='|' read -r name local_port resource remote_port proto mode <<< "$mapping"
    if ! start_one "$name" "$local_port" "$resource" "$remote_port" "$proto" "$mode"; then
      failures=$((failures + 1))
    fi
  done

  log "Done. Failures: $failures"
  return 0
}

usage() {
  cat <<EOF
Usage: $(basename "$0") {start|stop|restart|status}

Environment:
  NAMESPACE   Kubernetes namespace (default: odp-dev)
  STATE_DIR   State/log directory (default: /tmp/ai_trial_k8s_port_forward)
EOF
}

cmd="${1:-start}"
case "$cmd" in
  start)
    start_all
    ;;
  stop)
    stop_all
    ;;
  restart)
    stop_all
    start_all
    ;;
  status)
    status_all
    ;;
  *)
    usage
    exit 1
    ;;
esac
