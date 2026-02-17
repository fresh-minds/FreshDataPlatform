#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_ID="${E2E_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
RUN_DIR="$ROOT_DIR/tests/e2e/evidence/$RUN_ID"
LATEST_DIR="$ROOT_DIR/tests/e2e/evidence/latest"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  source "$ROOT_DIR/.env"
  set +a
fi

mkdir -p "$RUN_DIR/logs" "$RUN_DIR/queries" "$RUN_DIR/results" "$RUN_DIR/inventory"
mkdir -p "$RUN_DIR/screenshots"

cat > "$RUN_DIR/results/run_manifest.json" <<EOF
{
  "run_id": "$RUN_ID",
  "started_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
  "repo_root": "$ROOT_DIR"
}
EOF

# Prepare deterministic fixtures and inventory snapshot.
"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/fixtures/generate_e2e_fixtures.py" > "$RUN_DIR/logs/fixtures_generation.log" 2>&1
"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/e2e/scripts/inventory_platform.py" \
  --output-json "$RUN_DIR/inventory/platform_inventory.json" \
  --output-md "$RUN_DIR/inventory/platform_inventory.md" \
  > "$RUN_DIR/logs/inventory.log" 2>&1

# Ensure warehouse schemas are seeded via dbt parallel stack.
DBT_BIN="$ROOT_DIR/.venv/bin/dbt"
if [[ -x "$DBT_BIN" ]]; then
  "$DBT_BIN" seed --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --full-refresh \
    > "$RUN_DIR/logs/dbt_seed.log" 2>&1
  "$DBT_BIN" run --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
    > "$RUN_DIR/logs/dbt_run.log" 2>&1
  "$DBT_BIN" snapshot --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
    > "$RUN_DIR/logs/dbt_snapshot.log" 2>&1
  "$DBT_BIN" test --project-dir "$ROOT_DIR/dbt_parallel" --profiles-dir "$ROOT_DIR/dbt_parallel" --vars '{use_seed_data: true}' \
    > "$RUN_DIR/logs/dbt_test.log" 2>&1
fi

# Seed base schemas from dbt outputs for local warehouse parity.
"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/seed_warehouse_base_schemas.py" \
  > "$RUN_DIR/logs/seed_warehouse_base.log" 2>&1

# Capture UI screenshot evidence for serving/governance surfaces.
"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/e2e/scripts/capture_ui_screenshots.py" \
  --output-dir "$RUN_DIR/screenshots" \
  > "$RUN_DIR/logs/screenshots.log" 2>&1

# Capture baseline platform health (non-blocking; assertions happen inside pytest).
set +e
"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/scripts/verify_platform.py" > "$RUN_DIR/logs/platform_health.log" 2>&1
VERIFY_EXIT=$?
set -e

# Critical-path tests first (fail-fast stage), then non-critical for partial coverage.
set +e
E2E_EVIDENCE_DIR="$RUN_DIR" "$ROOT_DIR/.venv/bin/pytest" "$ROOT_DIR/tests/e2e/suite" -m critical -vv \
  --junitxml "$RUN_DIR/results/critical-junit.xml" \
  | tee "$RUN_DIR/logs/pytest-critical.log"
CRITICAL_EXIT=${PIPESTATUS[0]}

E2E_EVIDENCE_DIR="$RUN_DIR" "$ROOT_DIR/.venv/bin/pytest" "$ROOT_DIR/tests/e2e/suite" -m "not critical" -vv \
  --junitxml "$RUN_DIR/results/noncritical-junit.xml" \
  | tee "$RUN_DIR/logs/pytest-noncritical.log"
NONCRITICAL_EXIT=${PIPESTATUS[0]}
set -e

"$ROOT_DIR/.venv/bin/python" "$ROOT_DIR/tests/e2e/scripts/summarize_e2e_results.py" \
  --results-jsonl "$RUN_DIR/results/test_results.jsonl" \
  --output-json "$RUN_DIR/results/summary.json" \
  --output-md "$RUN_DIR/results/summary.md" \
  > "$RUN_DIR/logs/summary.log" 2>&1

cat > "$RUN_DIR/results/run_status.json" <<EOF
{
  "run_id": "$RUN_ID",
  "verify_platform_exit": $VERIFY_EXIT,
  "critical_exit": $CRITICAL_EXIT,
  "noncritical_exit": $NONCRITICAL_EXIT,
  "finished_at_utc": "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
}
EOF

# Keep a stable pointer to latest run for humans and CI artifacts.
rm -rf "$LATEST_DIR"
cp -R "$RUN_DIR" "$LATEST_DIR"

if [[ $CRITICAL_EXIT -ne 0 || $NONCRITICAL_EXIT -ne 0 ]]; then
  echo "E2E suite completed with failures. Evidence: $RUN_DIR"
  exit 1
fi

echo "E2E suite passed. Evidence: $RUN_DIR"
