#!/usr/bin/env bash
set -euo pipefail

NAMESPACE="${NAMESPACE:-ingress-nginx}"
SERVICE_NAME="${SERVICE_NAME:-ingress-nginx-controller}"
LOCAL_PORT="${LOCAL_PORT:-8085}"
REMOTE_PORT="${REMOTE_PORT:-80}"
STATE_DIR="${STATE_DIR:-/tmp/ai_trial_k8s_ingress_port_forward}"
PID_FILE="$STATE_DIR/port-forward.pid"
LOG_FILE="$STATE_DIR/port-forward.log"

mkdir -p "$STATE_DIR"

is_pid_alive() {
  local pid="$1"
  kill -0 "$pid" >/dev/null 2>&1
}

start() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && is_pid_alive "$pid"; then
      echo "[k8s-ingress-port-forward] already running (pid $pid)"
      return 0
    fi
    rm -f "$PID_FILE"
  fi

  if lsof -nP -iTCP:"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "[k8s-ingress-port-forward] local port $LOCAL_PORT already in use"
    return 1
  fi

  nohup bash -lc "
    while true; do
      kubectl -n '$NAMESPACE' port-forward 'svc/$SERVICE_NAME' '$LOCAL_PORT:$REMOTE_PORT' >>'$LOG_FILE' 2>&1 || true
      sleep 1
    done
  " >/dev/null 2>&1 < /dev/null &
  local pid="$!"
  echo "$pid" >"$PID_FILE"
  sleep 0.4

  if is_pid_alive "$pid"; then
    echo "[k8s-ingress-port-forward] started localhost:$LOCAL_PORT -> svc/$SERVICE_NAME:$REMOTE_PORT (pid $pid)"
    return 0
  fi

  echo "[k8s-ingress-port-forward] failed to start (see $LOG_FILE)"
  rm -f "$PID_FILE"
  return 1
}

stop() {
  if [[ ! -f "$PID_FILE" ]]; then
    echo "[k8s-ingress-port-forward] not running"
    return 0
  fi

  local pid
  pid="$(cat "$PID_FILE" 2>/dev/null || true)"
  if [[ -n "$pid" ]] && is_pid_alive "$pid"; then
    kill "$pid" >/dev/null 2>&1 || true
    echo "[k8s-ingress-port-forward] stopped (pid $pid)"
  else
    echo "[k8s-ingress-port-forward] stale pid file removed"
  fi
  rm -f "$PID_FILE"
}

status() {
  if [[ -f "$PID_FILE" ]]; then
    local pid
    pid="$(cat "$PID_FILE" 2>/dev/null || true)"
    if [[ -n "$pid" ]] && is_pid_alive "$pid"; then
      echo "[k8s-ingress-port-forward] running (pid $pid) localhost:$LOCAL_PORT -> svc/$SERVICE_NAME:$REMOTE_PORT"
      return 0
    fi
  fi
  echo "[k8s-ingress-port-forward] stopped"
}

case "${1:-start}" in
  start)
    start
    ;;
  stop)
    stop
    ;;
  restart)
    stop
    start
    ;;
  status)
    status
    ;;
  *)
    echo "Usage: $(basename "$0") {start|stop|restart|status}" >&2
    exit 1
    ;;
esac
