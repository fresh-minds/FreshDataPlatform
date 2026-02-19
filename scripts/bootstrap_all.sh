#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

RESET=false
SKIP_DBT=false
SKIP_MINIO=false
SKIP_SUPERSET=false
SKIP_DATAHUB=false
SKIP_KEYCLOAK_CHECK=false
SKIP_DEV_INSTALL=false
AUTO_FILL_ENV=false

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
  --skip-dev-install  skip creating .venv + installing dev deps
  --auto-fill-env     auto-generate missing/placeholder secrets in .env
  --skip-dbt        skip dbt seed/run/snapshot/test (warehouse will be less complete)
  --skip-minio      skip uploading fixtures to MinIO
  --skip-superset   skip running Superset setup/bootstrap scripts
  --skip-datahub    skip DataHub metadata sync/registration
  --skip-keycloak-check  skip Keycloak/OIDC verification checks
  -h, --help        show this help
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --reset) RESET=true; shift ;;
    --skip-dev-install) SKIP_DEV_INSTALL=true; shift ;;
    --auto-fill-env) AUTO_FILL_ENV=true; shift ;;
    --skip-dbt) SKIP_DBT=true; shift ;;
    --skip-minio) SKIP_MINIO=true; shift ;;
    --skip-superset) SKIP_SUPERSET=true; shift ;;
    --skip-datahub) SKIP_DATAHUB=true; shift ;;
    --skip-keycloak-check) SKIP_KEYCLOAK_CHECK=true; shift ;;
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

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "Required command not found: $cmd" >&2
    exit 1
  fi
}

require_compose() {
  if command -v docker >/dev/null 2>&1; then
    return 0
  fi
  if command -v docker-compose >/dev/null 2>&1; then
    return 0
  fi
  echo "Docker compose not found (need 'docker' or 'docker-compose')" >&2
  exit 1
}

ensure_python_env() {
  if [[ "$SKIP_DEV_INSTALL" == "true" ]]; then
    return 0
  fi

  if [[ ! -d "$ROOT_DIR/.venv" ]]; then
    log "Creating .venv..."
    python3 -m venv "$ROOT_DIR/.venv"
  fi

  log "Installing dev dependencies (pip install -e .[dev])..."
  (cd "$ROOT_DIR" && "$ROOT_DIR/.venv/bin/python" -m pip install -e ".[dev]")
}

generate_secret() {
  python3 - <<'PY'
import base64, os
print(base64.urlsafe_b64encode(os.urandom(32)).decode())
PY
}

upsert_env() {
  local key="$1"
  local value="$2"
  ROOT_DIR="$ROOT_DIR" KEY="$key" VALUE="$value" python3 - <<'PY'
from os import getenv
from pathlib import Path

key = getenv("KEY")
value = getenv("VALUE")
path = Path(getenv("ROOT_DIR", ".")).joinpath(".env")
lines = path.read_text().splitlines() if path.exists() else []
out = []
found = False
for line in lines:
    if not line or line.lstrip().startswith("#"):
        out.append(line)
        continue
    k, sep, v = line.partition("=")
    if sep and k == key:
        out.append(f"{k}={value}")
        found = True
    else:
        out.append(line)
if not found:
    out.append(f"{key}={value}")
path.write_text("\n".join(out) + "\n")
PY
}

ensure_env_file() {
  if [[ ! -f "$ROOT_DIR/.env" ]]; then
    if [[ ! -f "$ROOT_DIR/.env.template" ]]; then
      echo ".env not found and .env.template missing" >&2
      exit 1
    fi
    cp "$ROOT_DIR/.env.template" "$ROOT_DIR/.env"
    log "Created .env from .env.template"
    AUTO_FILL_ENV=true
  fi
}

ensure_env_secrets() {
  if [[ "$AUTO_FILL_ENV" != "true" ]]; then
    return 0
  fi

  local fernet_key
  fernet_key="${AIRFLOW_FERNET_KEY:-}"
  if [[ -z "$fernet_key" || "$fernet_key" == change_me_* ]]; then
    log "Generating AIRFLOW_FERNET_KEY..."
    upsert_env "AIRFLOW_FERNET_KEY" "$(generate_secret)"
  fi

  local superset_secret
  superset_secret="${SUPERSET_SECRET_KEY:-}"
  if [[ -z "$superset_secret" || "$superset_secret" == change_me_* ]]; then
    log "Generating SUPERSET_SECRET_KEY..."
    upsert_env "SUPERSET_SECRET_KEY" "$(generate_secret)"
  fi

  local datahub_secret
  datahub_secret="${DATAHUB_FRONTEND_SECRET:-}"
  if [[ -z "$datahub_secret" || "$datahub_secret" == change_me_* ]]; then
    log "Generating DATAHUB_FRONTEND_SECRET..."
    upsert_env "DATAHUB_FRONTEND_SECRET" "$(generate_secret)"
  fi

  local minio_user
  minio_user="${MINIO_ROOT_USER:-}"
  if [[ -z "$minio_user" || "$minio_user" == change_me_* || "${#minio_user}" -lt 3 ]]; then
    log "Setting MINIO_ROOT_USER..."
    upsert_env "MINIO_ROOT_USER" "minioadmin"
  fi

  local minio_pass
  minio_pass="${MINIO_ROOT_PASSWORD:-}"
  if [[ -z "$minio_pass" || "$minio_pass" == change_me_* || "${#minio_pass}" -lt 8 ]]; then
    log "Generating MINIO_ROOT_PASSWORD..."
    upsert_env "MINIO_ROOT_PASSWORD" "$(generate_secret)"
  fi

  local minio_access
  minio_access="${MINIO_ACCESS_KEY:-}"
  if [[ -z "$minio_access" || "$minio_access" == change_me_* ]]; then
    log "Setting MINIO_ACCESS_KEY to MINIO_ROOT_USER..."
    upsert_env "MINIO_ACCESS_KEY" "${MINIO_ROOT_USER:-minioadmin}"
  fi

  local minio_secret
  minio_secret="${MINIO_SECRET_KEY:-}"
  if [[ -z "$minio_secret" || "$minio_secret" == change_me_* || "${#minio_secret}" -lt 8 ]]; then
    log "Setting MINIO_SECRET_KEY to MINIO_ROOT_PASSWORD..."
    upsert_env "MINIO_SECRET_KEY" "${MINIO_ROOT_PASSWORD:-}"
  fi
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

wait_for_container_exit_success() {
  local container="$1"
  local timeout_s="${2:-240}"

  log "Waiting for container to exit successfully: ${container} (timeout ${timeout_s}s)"
  local start
  start="$(date +%s)"

  while true; do
    local status
    status="$(docker inspect -f '{{.State.Status}}' "$container" 2>/dev/null || true)"
    if [[ "$status" == "exited" ]]; then
      local exit_code
      exit_code="$(docker inspect -f '{{.State.ExitCode}}' "$container" 2>/dev/null || echo 1)"
      if [[ "$exit_code" == "0" ]]; then
        log "${container} exited successfully."
        return 0
      fi
      log "${container} exited with code ${exit_code}."
      return 1
    fi

    local now
    now="$(date +%s)"
    if (( now - start > timeout_s )); then
      log "Timed out waiting for ${container} to exit."
      return 1
    fi
    sleep 3
  done
}

reset_datahub_mysql_volume() {
  log "Resetting DataHub MySQL volume to recover from failed setup..."
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" rm -f -v datahub-mysql-setup datahub-mysql || true
  local volumes
  volumes="$(docker volume ls --format '{{.Name}}' | grep -E '(^|_)datahub-mysql-volume$' || true)"
  if [[ -n "$volumes" ]]; then
    echo "$volumes" | xargs docker volume rm >/dev/null 2>&1 || true
  fi
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d datahub-mysql
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d datahub-mysql-setup
}

ensure_datahub_mysql_setup() {
  if ! wait_for_container_exit_success "datahub-mysql-setup" 240; then
    local logs
    logs="$(docker logs --tail=200 datahub-mysql-setup 2>/dev/null || true)"
    if echo "$logs" | grep -q "Access denied for user 'root'"; then
      reset_datahub_mysql_volume
      wait_for_container_exit_success "datahub-mysql-setup" 240
      return $?
    fi
    return 1
  fi
}

reset_airflow_postgres_volume() {
  log "Resetting Airflow Postgres volume to recover from auth failures..."
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" rm -f -v airflow-init airflow-webserver airflow-scheduler postgres || true
  local volumes
  volumes="$(docker volume ls --format '{{.Name}}' | grep -E '(^|_)postgres-db-volume$' || true)"
  if [[ -n "$volumes" ]]; then
    echo "$volumes" | xargs docker volume rm >/dev/null 2>&1 || true
  fi
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d postgres
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d airflow-init
}

ensure_airflow_init() {
  if ! wait_for_container_exit_success "airflow-init" 240; then
    local logs
    logs="$(docker logs --tail=200 airflow-init 2>/dev/null || true)"
    if echo "$logs" | grep -q "password authentication failed"; then
      reset_airflow_postgres_volume
      wait_for_container_exit_success "airflow-init" 240
      return $?
    fi
    return 1
  fi
}

reset_datahub_kafka() {
  log "Resetting DataHub Kafka/Zookeeper containers to recover from broker registration errors..."
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" rm -f -v datahub-kafka-setup datahub-schema-registry datahub-kafka datahub-zookeeper || true
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d datahub-zookeeper datahub-kafka datahub-schema-registry datahub-kafka-setup
}

ensure_datahub_kafka() {
  local status
  status="$(docker inspect -f '{{.State.Status}}' datahub-kafka 2>/dev/null || true)"
  if [[ "$status" == "exited" ]]; then
    local logs
    logs="$(docker logs --tail=200 datahub-kafka 2>/dev/null || true)"
    if echo "$logs" | grep -q "NodeExistsException"; then
      reset_datahub_kafka
    fi
  fi
}

require_compose
require_cmd curl
require_cmd python3

ensure_env_file

# Load .env for local tooling (dbt + scripts). docker compose reads it automatically.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

ensure_env_secrets

# Reload .env if we modified it.
if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

ensure_python_env

PYTHON="$ROOT_DIR/.venv/bin/python"
if [[ ! -x "$PYTHON" ]]; then
  PYTHON="$(command -v python3 || true)"
fi
if [[ -z "${PYTHON:-}" ]]; then
  echo "python3 not found (expected .venv or system python3)" >&2
  exit 1
fi

if [[ "$RESET" == "true" ]]; then
  log "Reset requested: bringing stack down and removing volumes..."
  $COMPOSE -f "$ROOT_DIR/docker-compose.yml" down -v --remove-orphans || true
fi

log "Starting docker compose stack..."
$COMPOSE -f "$ROOT_DIR/docker-compose.yml" up -d

# Ensure DataHub Kafka is healthy (can fail on stale ZK broker registration).
ensure_datahub_kafka

# Ensure DataHub MySQL setup completed (can fail on old volumes).
ensure_datahub_mysql_setup
# Ensure Airflow init completed (can fail on old volumes).
ensure_airflow_init

# Core readiness checks
wait_for_http "MinIO" "http://localhost:9000/minio/health/live" 240
wait_for_http "MinIO SSO Bridge" "http://localhost:9011/healthz" 240
wait_for_container_health "open-data-platform-warehouse" 240
wait_for_http "Superset" "http://localhost:8088/health" 360 || wait_for_http "Superset" "http://localhost:8088/login/" 360
wait_for_http "DataHub GMS" "http://localhost:8081/health" 420

if [[ "$SKIP_KEYCLOAK_CHECK" != "true" ]]; then
  log "Verifying Keycloak + OIDC resources (Airflow/DataHub/MinIO clients)..."
  "$PYTHON" "$ROOT_DIR/scripts/verify_keycloak_resources.py" --timeout 20 --retries 30 --retry-delay 3
else
  log "Skipping Keycloak verification (--skip-keycloak-check)."
fi

if [[ "$SKIP_MINIO" != "true" ]]; then
  log "Populating MinIO buckets with deterministic fixture files..."
  if [[ -f "$ROOT_DIR/tests/fixtures/generate_e2e_fixtures.py" ]]; then
    "$PYTHON" "$ROOT_DIR/tests/fixtures/generate_e2e_fixtures.py" >/dev/null 2>&1 || true
  else
    log "Fixture generator missing: tests/fixtures/generate_e2e_fixtures.py (skipping generation)."
  fi

  # Use the MinIO root credentials by default for bootstrap uploads.
  export MINIO_ACCESS_KEY="${MINIO_ROOT_USER:-${MINIO_ACCESS_KEY:-}}"
  export MINIO_SECRET_KEY="${MINIO_ROOT_PASSWORD:-${MINIO_SECRET_KEY:-}}"

  if [[ -d "$ROOT_DIR/tests/fixtures/generated" ]]; then
    "$PYTHON" "$ROOT_DIR/scripts/populate_minio_fixtures.py" \
      --fixtures-dir "$ROOT_DIR/tests/fixtures/generated" \
      --bucket "bronze" \
      --prefix "fixtures/generated"
  else
    log "Skipping MinIO generated fixtures; missing tests/fixtures/generated."
  fi

  if [[ -d "$ROOT_DIR/tests/fixtures/golden" ]]; then
    "$PYTHON" "$ROOT_DIR/scripts/populate_minio_fixtures.py" \
      --fixtures-dir "$ROOT_DIR/tests/fixtures/golden" \
      --bucket "gold" \
      --prefix "fixtures/golden"
  else
    log "Skipping MinIO golden fixtures; missing tests/fixtures/golden."
  fi

  if [[ "${JOB_MARKET_USE_SPARK:-false}" == "true" ]]; then
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
    log "JOB_MARKET_USE_SPARK!=true; skipping Spark-based Delta writes to MinIO."
  fi
else
  log "Skipping MinIO population (--skip-minio)."
fi

if [[ "$SKIP_DBT" != "true" ]]; then
  log "Ensuring job market source tables exist for dbt sources..."
  "$PYTHON" "$ROOT_DIR/scripts/ensure_job_market_source_tables.py"

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
  "$PYTHON" "$ROOT_DIR/scripts/refresh_datahub_catalog.py" || true
else
  log "Skipping DataHub bootstrap (--skip-datahub)."
fi

log "Verifying platform endpoints..."
"$PYTHON" "$ROOT_DIR/scripts/verify_platform.py" || true

log "Bootstrap complete."
log "Superset: http://localhost:8088 (user: ${SUPERSET_ADMIN_USER:-admin})"
log "DataHub:  http://localhost:9002"
log "MinIO:    http://localhost:9001"
log "MinIO SSO Bridge: http://localhost:9011"
log "Airflow:  http://localhost:8080"
