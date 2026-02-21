#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$ROOT_DIR"

LOOKBACK_SECONDS="${OBS_LOOKBACK_SECONDS:-900}"
REQUIRE_TRACE_VOLUME="${OBS_REQUIRE_TRACE_VOLUME:-false}"
TRACE_VOLUME_WINDOW_SECONDS="${OBS_TRACE_VOLUME_WINDOW_SECONDS:-30}"
TRACE_VOLUME_MIN_SPANS="${OBS_TRACE_VOLUME_MIN_SPANS:-10}"
TRACE_VOLUME_PROBE_INTERVAL_SECONDS="${OBS_TRACE_VOLUME_PROBE_INTERVAL_SECONDS:-1}"
TRACE_VOLUME_MODE="${OBS_TRACE_VOLUME_MODE:-synthetic}"

log() {
  echo "[verify-compose-observability] $*"
}

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

require_cmd docker
require_cmd curl
require_cmd python3

wait_for_http_ok() {
  local url="$1"
  local timeout_seconds="${2:-60}"
  local deadline=$(( $(date +%s) + timeout_seconds ))

  while (( $(date +%s) <= deadline )); do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done

  echo "Timed out waiting for endpoint: $url" >&2
  return 1
}

check_service_running() {
  local name="$1"
  local state
  state="$(docker inspect -f '{{.State.Status}}' "$name" 2>/dev/null || true)"
  [[ "$state" == "running" ]]
}

expect_json_non_empty_result() {
  local payload="$1"
  JSON_PAYLOAD="$payload" python3 - <<'PY'
import json
import os
import sys

payload = os.environ.get("JSON_PAYLOAD", "")
try:
    obj = json.loads(payload)
except Exception:
    sys.exit(1)

if obj.get("status") != "success":
    sys.exit(2)

result = obj.get("data", {}).get("result", [])
if not result:
    sys.exit(3)
PY
}

expect_tempo_trace_found() {
  local payload="$1"
  JSON_PAYLOAD="$payload" python3 - <<'PY'
import json
import os
import sys

payload = os.environ.get("JSON_PAYLOAD", "")
try:
  obj = json.loads(payload)
except Exception:
  sys.exit(1)

if obj.get("status") != "success":
  pass

batches = obj.get("batches", [])
if not batches:
  sys.exit(3)
PY
}

emit_otlp_trace() {
  local tmp trace_id payload
  tmp="$(mktemp)"
  python3 - <<'PY' > "$tmp"
import json
import secrets
import time

trace_id = secrets.token_hex(16)
start_ns = time.time_ns() - 5_000_000
end_ns = time.time_ns()

trace_payload = {
  "resourceSpans": [
    {
      "resource": {
        "attributes": [
          {"key": "service.name", "value": {"stringValue": "compose-observability-verifier"}},
          {"key": "deployment.environment", "value": {"stringValue": "local"}},
        ]
      },
      "scopeSpans": [
        {
          "scope": {"name": "verify_compose_observability.sh"},
          "spans": [
            {
              "traceId": trace_id,
              "spanId": secrets.token_hex(8),
              "name": "compose_observability_trace_check",
              "kind": 1,
              "startTimeUnixNano": str(start_ns),
              "endTimeUnixNano": str(end_ns),
              "attributes": [
                {"key": "verifier", "value": {"stringValue": "scripts/testing/verify_compose_observability.sh"}}
              ],
            }
          ],
        }
      ],
    }
  ]
}

print(trace_id)
print(json.dumps(trace_payload, separators=(",", ":")))
PY

  trace_id="$(sed -n '1p' "$tmp")"
  payload="$(sed -n '2p' "$tmp")"
  rm -f "$tmp"

  curl -fsS \
  -H 'Content-Type: application/json' \
  -X POST \
  http://localhost:4320/v1/traces \
  -d "$payload" >/dev/null

  printf '%s\n' "$trace_id"
}

get_tempo_spans_received_total() {
  local metrics
  metrics="$(curl -fsS http://localhost:3200/metrics)"
  METRICS_PAYLOAD="$metrics" python3 - <<'PY'
import os
import re
import sys

payload = os.environ.get("METRICS_PAYLOAD", "")
if not payload:
    sys.exit(1)

total = 0.0
matches = re.findall(r'^tempo_distributor_spans_received_total\{[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)\s*$', payload, flags=re.MULTILINE)
if matches:
    total = sum(float(x) for x in matches)
else:
    matches = re.findall(r'^tempo_receiver_accepted_spans\{[^}]*\}\s+([0-9]+(?:\.[0-9]+)?)\s*$', payload, flags=re.MULTILINE)
    if not matches:
        sys.exit(2)
    total = sum(float(x) for x in matches)

print(f"{total:.0f}")
PY
}

assert_trace_volume() {
  local start_counter end_counter delta
  local probes=0

  if [[ "$TRACE_VOLUME_MODE" != "synthetic" && "$TRACE_VOLUME_MODE" != "ambient" ]]; then
    echo "Invalid OBS_TRACE_VOLUME_MODE='$TRACE_VOLUME_MODE'. Supported values: synthetic, ambient" >&2
    exit 1
  fi

  log "Strict mode: checking minimum trace volume over ${TRACE_VOLUME_WINDOW_SECONDS}s (min ${TRACE_VOLUME_MIN_SPANS} spans, mode=${TRACE_VOLUME_MODE})."
  start_counter="$(get_tempo_spans_received_total)"

  local deadline=$(( $(date +%s) + TRACE_VOLUME_WINDOW_SECONDS ))
  while (( $(date +%s) < deadline )); do
    if [[ "$TRACE_VOLUME_MODE" == "synthetic" ]]; then
      emit_otlp_trace >/dev/null
      probes=$((probes + 1))
    fi
    sleep "$TRACE_VOLUME_PROBE_INTERVAL_SECONDS"
  done

  end_counter="$(get_tempo_spans_received_total)"
  delta=$((end_counter - start_counter))

  if (( delta < TRACE_VOLUME_MIN_SPANS )); then
    echo "Trace volume check failed: observed ${delta} spans over ${TRACE_VOLUME_WINDOW_SECONDS}s, required >= ${TRACE_VOLUME_MIN_SPANS}." >&2
    echo "Try increasing trace emit activity or adjusting OBS_TRACE_VOLUME_* thresholds." >&2
    exit 1
  fi

  if [[ "$TRACE_VOLUME_MODE" == "synthetic" ]]; then
    log "Trace volume check passed: observed ${delta} spans over ${TRACE_VOLUME_WINDOW_SECONDS}s (probes sent: ${probes})."
  else
    log "Trace volume check passed: observed ${delta} ambient spans over ${TRACE_VOLUME_WINDOW_SECONDS}s."
  fi
}

for svc in open-data-platform-grafana open-data-platform-loki open-data-platform-prometheus open-data-platform-promtail open-data-platform-statsd-exporter open-data-platform-otel-collector open-data-platform-tempo; do
  if ! check_service_running "$svc"; then
    echo "Required service is not running: $svc" >&2
    echo "Start stack first, for example: docker compose up -d grafana loki prometheus promtail statsd-exporter otel-collector tempo airflow-webserver airflow-scheduler" >&2
    exit 1
  fi
done

log "Checking endpoint health..."
wait_for_http_ok http://localhost:3001/api/health 60
wait_for_http_ok http://localhost:3100/ready 60
wait_for_http_ok http://localhost:9090/-/healthy 60
wait_for_http_ok http://localhost:3200/ready 90

log "Checking Grafana datasource connectivity..."
GRAFANA_USER="$(docker exec open-data-platform-grafana printenv GF_SECURITY_ADMIN_USER)"
GRAFANA_PASS="$(docker exec open-data-platform-grafana printenv GF_SECURITY_ADMIN_PASSWORD)"
curl -fsS -u "$GRAFANA_USER:$GRAFANA_PASS" http://localhost:3001/api/datasources/uid/PBFA97CFB590B2093/health >/dev/null
curl -fsS -u "$GRAFANA_USER:$GRAFANA_PASS" http://localhost:3001/api/datasources/uid/P8E80F9AEF21F6940/health >/dev/null

log "Checking Prometheus scrape targets are up..."
PROM_UP_JSON="$(curl -fsSG 'http://localhost:9090/api/v1/query' --data-urlencode 'query=up{job=~"prometheus|alertmanager|statsd-exporter|otel-collector|minio"}')"
expect_json_non_empty_result "$PROM_UP_JSON"

log "Checking Airflow metrics exist in Prometheus..."
AIRFLOW_METRICS_JSON="$(curl -fsSG 'http://localhost:9090/api/v1/query' --data-urlencode 'query={__name__=~"airflow_.*"}')"
expect_json_non_empty_result "$AIRFLOW_METRICS_JSON"

if [[ "$REQUIRE_TRACE_VOLUME" == "true" && "$TRACE_VOLUME_MODE" == "ambient" ]]; then
  log "Ambient strict mode selected: skipping synthetic trace injection/retrieval checks."
else
  log "Sending a synthetic OTLP trace via the collector..."
  TRACE_ID="$(emit_otlp_trace)"

  log "Checking Tempo can return the injected trace..."
  TRACE_FOUND=false
  for _ in {1..20}; do
    TRACE_JSON="$(curl -sS "http://localhost:3200/api/traces/$TRACE_ID" || true)"
    if expect_tempo_trace_found "$TRACE_JSON"; then
      TRACE_FOUND=true
      break
    fi
    sleep 1
  done

  if [[ "$TRACE_FOUND" != "true" ]]; then
    echo "Failed to find injected trace '$TRACE_ID' in Tempo query API." >&2
    exit 1
  fi
fi

log "Checking Loki has Airflow file logs..."
START_NS="$(( $(date +%s) - LOOKBACK_SECONDS ))000000000"
END_NS="$(date +%s)000000000"
LOKI_AIRFLOW_JSON="$(curl -fsSG 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={job="airflow"}' \
  --data-urlencode "start=$START_NS" \
  --data-urlencode "end=$END_NS" \
  --data-urlencode 'limit=5')"
expect_json_non_empty_result "$LOKI_AIRFLOW_JSON"

log "Checking Loki has Docker stdout logs..."
LOKI_DOCKER_JSON="$(curl -fsSG 'http://localhost:3100/loki/api/v1/query_range' \
  --data-urlencode 'query={job="docker"}' \
  --data-urlencode "start=$START_NS" \
  --data-urlencode "end=$END_NS" \
  --data-urlencode 'limit=5')"
expect_json_non_empty_result "$LOKI_DOCKER_JSON"

if [[ "$REQUIRE_TRACE_VOLUME" == "true" ]]; then
  assert_trace_volume
fi

log "All checks passed. Compose observability ingestion is working (Grafana/Loki/Prometheus/Tempo)."
