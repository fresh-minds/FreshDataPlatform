#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT_DIR="$ROOT_DIR/dbt"
PROFILES_DIR="$ROOT_DIR/dbt"
DBT_BIN="$ROOT_DIR/.venv/bin/dbt"
DBT_THREADS="${DBT_THREADS:-1}"

run_dbt() {
  local subcmd="$1"
  shift
  local -a base_args
  base_args=("$DBT_BIN" "$subcmd" --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR")
  case "$subcmd" in
    run|seed|snapshot|test)
      base_args+=(--threads "$DBT_THREADS")
      ;;
  esac
  "${base_args[@]}" "$@"
}

echo "[dbt] Running with DBT_THREADS=$DBT_THREADS"
run_dbt debug
run_dbt deps
run_dbt seed --full-refresh
run_dbt run --vars '{use_seed_data: true}'
run_dbt snapshot --vars '{use_seed_data: true}'
run_dbt test --vars '{use_seed_data: true}'
