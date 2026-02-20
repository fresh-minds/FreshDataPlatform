#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
ARTIFACT_ROOT="${SSO_ARTIFACT_ROOT:-$ROOT_DIR/tests/sso/artifacts}"
RUN_ID="${SSO_RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)}"
JUNIT_PATH="$ARTIFACT_ROOT/junit-$RUN_ID.xml"
LOG_PATH="$ARTIFACT_ROOT/pytest-$RUN_ID.log"

mkdir -p "$ARTIFACT_ROOT"

if [[ -f "$ROOT_DIR/.env" ]]; then
  set -a
  # shellcheck disable=SC1091
  source "$ROOT_DIR/.env"
  set +a
fi

export SSO_ARTIFACT_ROOT="$ARTIFACT_ROOT"

PYTEST_BIN="$ROOT_DIR/.venv/bin/pytest"
PYTHON_BIN="$ROOT_DIR/.venv/bin/python"
PLAYWRIGHT_BIN="$ROOT_DIR/.venv/bin/playwright"

if [[ ! -x "$PYTEST_BIN" ]]; then
  PYTEST_BIN="pytest"
fi
if [[ ! -x "$PYTHON_BIN" ]]; then
  PYTHON_BIN="python3"
fi

if [[ "${SSO_INSTALL_PLAYWRIGHT_BROWSERS:-false}" =~ ^(1|true|yes|on)$ ]]; then
  if [[ -x "$PLAYWRIGHT_BIN" ]]; then
    "$PLAYWRIGHT_BIN" install --with-deps chromium webkit
  else
    "$PYTHON_BIN" -m playwright install --with-deps chromium webkit
  fi
fi

set +e
"$PYTEST_BIN" tests/sso -vv --junitxml "$JUNIT_PATH" | tee "$LOG_PATH"
PYTEST_EXIT=${PIPESTATUS[0]}
set -e

LATEST_DIR="$ARTIFACT_ROOT/latest"
if [[ -d "$LATEST_DIR" ]]; then
  cp "$JUNIT_PATH" "$LATEST_DIR/junit.xml" || true
  cp "$LOG_PATH" "$LATEST_DIR/pytest.log" || true
fi

"$PYTHON_BIN" "$ROOT_DIR/scripts/sso/generate_report.py" \
  --junit-xml "$JUNIT_PATH" \
  --output "$ROOT_DIR/sso-test-report.md" \
  --artifact-dir "$LATEST_DIR" \
  --inventory "$ROOT_DIR/sso-test-inventory.md" \
  --environment "${SSO_ENVIRONMENT:-dev}"

if [[ "$PYTEST_EXIT" -ne 0 ]]; then
  echo "SSO tests failed. Report: $ROOT_DIR/sso-test-report.md"
  exit "$PYTEST_EXIT"
fi

echo "SSO tests passed. Report: $ROOT_DIR/sso-test-report.md"
