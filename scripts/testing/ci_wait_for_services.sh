#!/usr/bin/env bash
set -euo pipefail

max_attempts="${MAX_ATTEMPTS:-60}"
sleep_seconds="${SLEEP_SECONDS:-5}"

checks=(
  "http://localhost:8080/health"
  "http://localhost:8081/health"
  "http://localhost:9000/minio/health/live"
  "http://localhost:8088/login/"
)

for ((i=1; i<=max_attempts; i++)); do
  ready=true
  for url in "${checks[@]}"; do
    if ! curl -sf "$url" >/dev/null; then
      ready=false
      break
    fi
  done

  if [[ "$ready" == "true" ]]; then
    echo "Core endpoints are ready"
    exit 0
  fi

  echo "Waiting for services... ($i/$max_attempts)"
  sleep "$sleep_seconds"
done

echo "Timed out waiting for services" >&2
exit 1
