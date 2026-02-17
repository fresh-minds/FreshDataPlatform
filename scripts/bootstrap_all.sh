#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RESET=false
SKIP_DBT=false
SKIP_MINIO=false
SKIP_SUPERSET=false
SKIP_DATAHUB=false

usage() {
  cat <<EOF
Usage: $(basename "$0") [options]

Bootstraps the local Docker stack and (re)populates:
- MinIO buckets (uploads deterministic fixture files)
- Warehouse baseline schemas (dbt_parallel + base schemas)
- Superset (datasets + NL job market dashboard)
- DataHub (schema/glossary sync + warehouse catalog registration)

Options:
  --reset           docker compose down -v before starting
  --skip-dbt        skip dbt seed/run/snapshot/test (warehouse will be less complete)
  --skip-minio      skip uploading fixtures to MinIO
  --skip-superset   skip running Superset setup/bootstrap scripts
  --skip-datahub    skip DataHub metadata sync/registration
  -h, --help        show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset) RESET=true; shift ;;
    --skip-dbt) SKIP_DBT=true; shift ;;
    --skip-minio) SKIP_MINIO=true; shift ;;
    --skip-superset) SKIP_SUPERSET=true; shift ;;
    --skip-datahub) SKIP_DATAHUB=true; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Unknown arg: $1" >&2; usage; exit 2 ;;
  esac
done

compose_cmd() {
  if docker compose version >/dev/null 2>&1; then
    echo "docker compose"
  else
    echo "docker-compose"
  fi
}

COMPOSE="$(compose_cmd)"

log() {
  echo "[bootstrap] $*"
}

wait_for_http() {
  local name="$1"
  local url="$2"
  local timeout_s="${3:-240}"

  log "Waiting for ${name}: ${url} (timeout ${timeout_s}s)"
  local start
  start="$(date +%s)"

  while true; do
    if curl -fsS --max-time 5 "$url" >/dev/null 2>&1; then
      log "${name} is up."
      return 0
    fi
    local now
    now="$(date +%s)"
    if (( now - start > timeout_s )); then
      log "Timed out waiting for ${name}."
      return 1
    fi
    sleep 3
  done
}

wait_for_container_health() {
  local container="$1"
  local timeout_s="${2:-240}"

  log "Waiting for container health: ${container} (timeout ${timeout_s}s)"
  local start
  start="$(date +%s)"

  while true; do
    # healthy | unhealthy | starting | running | exited | <empty>
    local status
    status="$(docker inspect -f '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "healthy" ]]; then
      log "${container} is healthy."
      return 0
    fi
    if [[ "$status" == "running" ]]; then
      # No healthcheck configured; running is the best we can do.
      log "${container} is running (no healthcheck)."
      return 0
    fi

    local now
    now="$(date +%s)"
    if (( now - start > timeout_s )); then
      log "Timed out waiting for container: ${container} (status='${status}')"
      return 1
    fi
    sleep 3
  done
}

PYTHON="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON:-}" ]]; then
  echo "python3 not found (expected .venv or system python3)" >&2
  exit 1
fi

# Load .env for local tooling (dbt + scripts). docker compose reads it automatically.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

if [[ "$RESET" == "true" ]]; then
  log "Reset requested: bringing stack down and removing volumes..."
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" down -v --remove-orphans || true
fi

log "Starting docker compose stack..."
$COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d

# Core readiness checks
wait_for_http "MinIO" "http://localhost:9000/minio/health/live" 240
wait_for_container_health "open-data-platform-warehouse" 240
wait_for_http "Superset" "http://localhost:8088/health" 360 || wait_for_http "Superset" "http://localhost:8088/login/" 360
wait_for_http "DataHub GMS" "http://localhost:8081/health" 420

if [[ "$SKIP_MINIO" != "true" ]]; then
  log "Populating MinIO buckets with deterministic fixture files..."
  "$PYTHON" "$ROOT_DIR/tests/fixtures/generate_e2e_fixtures.py" >/dev/null 2>&1 || true

  # Use the MinIO root credentials by default for bootstrap uploads.
  export MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-${MINIO_ACCESS_KEY:-}}"
  export MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-${MINIO_SECRET_KEY:-}}"

  "$PYTHON" "$ROOT_DIR/scripts/populate_minio_fixtures.py" \
    --fixtures-dir "$ROOT_DIR/tests/fixtures/generated" \
    --bucket "bronze" \
    --prefix "fixtures/generated"

  "$PYTHON" "$ROOT_DIR/scripts/populate_minio_fixtures.py" \
    --fixtures-dir "$ROOT_DIR/tests/fixtures/golden" \
    --bucket "gold" \
    --prefix "fixtures/golden"

  if command -v java >/dev/null 2>&1 && java -version >/dev/null 2>&1; then
    log "Java runtime detected; generating a small Delta sample into MinIO (best-effort)..."
    USE_MINIO=true \
      MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-${MINIO_ACCESS_KEY:-}}" \
      MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-${MINIO_SECRET_KEY:-}}" \
      "$PYTHON" "$ROOT_DIR/scripts/generate_dummy_data.py" || true
  else
    log "Java runtime not available; skipping Spark-based Delta writes to MinIO."
  fi
else
  log "Skipping MinIO population (--skip-minio)."
fi

if [[ "$SKIP_DBT" != "true" ]]; then
  if [[ -x "$ROOT_DIR/.venv/bin/dbt" ]]; then
    log "Running dbt parallel stack (seed/run/snapshot/test)..."
    "$ROOT_DIR/scripts/run_dbt_parallel.sh"
  else
    log "dbt not found at $ROOT_DIR/.venv/bin/dbt; skipping dbt step."
  fi
else
  log "Skipping dbt step (--skip-dbt)."
fi

log "Seeding base serving schemas from dbt_parallel outputs..."
"$PYTHON" "$ROOT_DIR/scripts/seed_warehouse_base_schemas.py"

log "Applying warehouse security baseline (roles/grants/RLS/masking)..."
"$PYTHON" "$ROOT_DIR/scripts/apply_warehouse_security.py"

log "Populating job market demo tables in the warehouse (Spark-free pipeline)..."
"$PYTHON" "$ROOT_DIR/scripts/run_job_market_pipeline.py" || true

if [[ "$SKIP_SUPERSET" != "true" ]]; then
  log "Running Superset onboarding (datasets + job market dashboard)..."
  docker exec open-data-platform-superset python /app/scripts/superset_setup.py || true
  docker exec open-data-platform-superset python /app/scripts/superset_bootstrap_job_market.py || true
else
  log "Skipping Superset bootstrap (--skip-superset)."
fi

if [[ "$SKIP_DATAHUB" != "true" ]]; then
  log "Syncing governance + schema metadata into DataHub..."
  "$PYTHON" "$ROOT_DIR/scripts/create_governance_defs.py" || true
  "$PYTHON" "$ROOT_DIR/scripts/sync_dbml_to_datahub.py" || true
  "$PYTHON" "$ROOT_DIR/scripts/register_datahub_catalog.py" || true
else
  log "Skipping DataHub bootstrap (--skip-datahub)."
fi

log "Verifying platform endpoints..."
"$PYTHON" "$ROOT_DIR/scripts/verify_platform.py" || true

log "Bootstrap complete."
log "Superset: http://localhost:8088 (user: ${SUPERSET_ADMIN_USER:-admin})"
log "DataHub:  http://localhost:9002"
log "MinIO:    http://localhost:9001"
log "Airflow:  http://localhost:8080"
