#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_DIR="$ROOT_DIR/tests/e2e/evidence/$RUN_ID"
LATEST_DIR="$ROOT_DIR/tests/e2e/evidence/latest"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

# Host-run defaults for local docker-compose access.
if [[ "${WAREHOUSE_HOST:-}" == "warehouse" || -z "${WAREHOUSE_HOST:-}" ]]; then
  export WAREHOUSE_HOST="localhost"
fi
if [[ "${WAREHOUSE_PORT:-}" == "5432" || -z "${WAREHOUSE_PORT:-}" ]]; then
  export WAREHOUSE_PORT="5433"
fi

ensure_warehouse_credentials() {
  local container_name="open-data-platform-warehouse"
  if ! docker ps --format '{{.Names}}' | grep -qx "$container_name"; then
    return 0
  fi

  local target_user="${WAREHOUSE_USER:-admin}"
  local target_password="${WAREHOUSE_PASSWORD:-admin}"
  local target_db="${WAREHOUSE_DB:-open_data_platform_dw}"
  local admin_role=""

  for candidate in "$target_user" warehouse_app postgres admin; do
    if [[ -z "$candidate" ]]; then
      continue
    fi
    if docker exec "$container_name" sh -lc "psql -U \"$candidate\" -d postgres -c 'SELECT 1' >/dev/null 2>&1"; then
      admin_role="$candidate"
      break
    fi
    if docker exec "$container_name" sh -lc "psql -U \"$candidate\" -d \"$target_db\" -c 'SELECT 1' >/dev/null 2>&1"; then
      admin_role="$candidate"
      break
    fi
  done

  if [[ -z "$admin_role" ]]; then
    echo "[e2e] Could not identify a bootstrap role inside $container_name; continuing with current env credentials."
    return 0
  fi

  local escaped_password="${target_password//\'/\'\'}"
  docker exec "$container_name" sh -lc "psql -U \"$admin_role\" -d postgres -c \"DO \\\$\\\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '$target_user') THEN CREATE ROLE \\\"$target_user\\\" LOGIN SUPERUSER; END IF; ALTER ROLE \\\"$target_user\\\" WITH LOGIN SUPERUSER PASSWORD '$escaped_password'; END \\\$\\\$;\""

  local db_exists
  db_exists="$(docker exec "$container_name" sh -lc "psql -U \"$admin_role\" -d postgres -tAc \"SELECT 1 FROM pg_database WHERE datname='$target_db'\"")"
  if [[ "$db_exists" != "1" ]]; then
    docker exec "$container_name" sh -lc "psql -U \"$admin_role\" -d postgres -c \"CREATE DATABASE \\\"$target_db\\\" OWNER \\\"$target_user\\\";\""
  fi
}

ensure_warehouse_credentials

mkdir -p "$RUN_DIR/logs" "$RUN_DIR/results" "$RUN_DIR/dbt"

STARTED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
cat > "$RUN_DIR/results/run_manifest.json" <<JSON
{
  "run_id": "$RUN_ID",
  "started_at_utc": "$STARTED_AT_UTC",
  "repo_root": "$ROOT_DIR",
  "qa_env": "${QA_ENV:-test}"
}
JSON

DBT_BIN="$ROOT_DIR/.venv/bin/dbt"
PYTEST_BIN="$ROOT_DIR/.venv/bin/pytest"

if [[ ! -x "$DBT_BIN" ]]; then
  echo "dbt not found at $DBT_BIN" >&2
  exit 2
fi
if [[ ! -x "$PYTEST_BIN" ]]; then
  echo "pytest not found at $PYTEST_BIN" >&2
  exit 2
fi

set +e
"$DBT_BIN" debug --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" \
  > "$RUN_DIR/logs/dbt-debug.log" 2>&1
DBT_DEBUG_EXIT=$?

"$DBT_BIN" seed --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --full-refresh \
  > "$RUN_DIR/logs/dbt-seed.log" 2>&1
DBT_SEED_EXIT=$?

"$DBT_BIN" run --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
  > "$RUN_DIR/logs/dbt-run.log" 2>&1
DBT_RUN_EXIT=$?

"$DBT_BIN" snapshot --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
  > "$RUN_DIR/logs/dbt-snapshot.log" 2>&1
DBT_SNAPSHOT_EXIT=$?

"$DBT_BIN" test --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
  > "$RUN_DIR/logs/dbt-test.log" 2>&1
DBT_TEST_EXIT=$?
set -e

DBT_EXIT=0
for exit_code in "$DBT_DEBUG_EXIT" "$DBT_SEED_EXIT" "$DBT_RUN_EXIT" "$DBT_SNAPSHOT_EXIT" "$DBT_TEST_EXIT"; do
  if [[ "$exit_code" -ne 0 ]]; then
    DBT_EXIT=1
    break
  fi
done

if [[ "$DBT_EXIT" -eq 0 ]]; then
  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/seed_warehouse_base_schemas.py" \
    > "$RUN_DIR/logs/seed_warehouse_base_schemas.log" 2>&1

  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/apply_warehouse_security.py" \
    > "$RUN_DIR/logs/apply_warehouse_security.log" 2>&1

  "$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/e2e/scripts/log_pipeline_run.py" \
    --run-id "$RUN_ID" \
    --pipeline-name "dbt_parallel_e2e_suite" \
    --status "RUNNING" \
    --started-at-utc "$STARTED_AT_UTC" \
    --finished-at-utc "$STARTED_AT_UTC" \
    > "$RUN_DIR/logs/audit_log_start.log" 2>&1 || true
fi

set +e
QA_ENABLE_REPORTING=true \
QA_ARTIFACT_DIR="$RUN_DIR/results" \
QA_ENV="${QA_ENV:-test}" \
QA_REQUIRE_SERVICES=true \
"$PYTEST_BIN" \
  "$ROOT_DIR/tests/data_quality" \
  "$ROOT_DIR/tests/contracts" \
  "$ROOT_DIR/tests/governance" \
  "$ROOT_DIR/tests/e2e" \
  -vv \
  --junitxml "$RUN_DIR/results/junit.xml" \
  | tee "$RUN_DIR/logs/pytest.log"
PYTEST_EXIT=${PIPESTATUS[0]}
set -e

FINISHED_AT_UTC="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PIPELINE_STATUS="SUCCESS"
if [[ "$DBT_EXIT" -ne 0 || "$PYTEST_EXIT" -ne 0 ]]; then
  PIPELINE_STATUS="FAILED"
fi

"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/e2e/scripts/log_pipeline_run.py" \
  --run-id "$RUN_ID" \
  --pipeline-name "dbt_parallel_e2e_suite" \
  --status "$PIPELINE_STATUS" \
  --started-at-utc "$STARTED_AT_UTC" \
  --finished-at-utc "$FINISHED_AT_UTC" \
  > "$RUN_DIR/logs/audit_log_end.log" 2>&1 || true

cat > "$RUN_DIR/results/run_status.json" <<JSON
{
  "run_id": "$RUN_ID",
  "dbt_debug_exit": $DBT_DEBUG_EXIT,
  "dbt_seed_exit": $DBT_SEED_EXIT,
  "dbt_run_exit": $DBT_RUN_EXIT,
  "dbt_snapshot_exit": $DBT_SNAPSHOT_EXIT,
  "dbt_test_exit": $DBT_TEST_EXIT,
  "pytest_exit": $PYTEST_EXIT,
  "status": "$PIPELINE_STATUS",
  "finished_at_utc": "$FINISHED_AT_UTC"
}
JSON

rm -rf "$LATEST_DIR"
cp -R "$RUN_DIR" "$LATEST_DIR"

if [[ "$DBT_EXIT" -ne 0 ]]; then
  echo "E2E failed during dbt setup. Evidence: $RUN_DIR" >&2
  exit 1
fi

if [[ "$PYTEST_EXIT" -ne 0 ]]; then
  echo "E2E tests failed. Evidence: $RUN_DIR" >&2
  exit 1
fi

echo "E2E suite passed. Evidence: $RUN_DIR"
