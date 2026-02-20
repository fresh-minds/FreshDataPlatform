#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PROJECT_DIR="$ROOT_DIR/dbt_parallel"
PROFILES_DIR="$ROOT_DIR/dbt_parallel"
DBT_BIN="$ROOT_DIR/.venv/bin/dbt"

$DBT_BIN debug --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR"
$DBT_BIN seed --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" --full-refresh
$DBT_BIN run --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" --vars '{use_seed_data: true}'
$DBT_BIN snapshot --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" --vars '{use_seed_data: true}'
$DBT_BIN test --project-dir "$PROJECT_DIR" --profiles-dir "$PROFILES_DIR" --vars '{use_seed_data: true}'
