#!/usr/bin/env bash
set -euo pipefail

COMPOSE_FILE="${1:-docker-compose.yml}"
FAIL_ON_RESTRICTIVE="${FAIL_ON_RESTRICTIVE:-false}"

if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "Missing compose file: $COMPOSE_FILE" >&2
  exit 1
fi

images=()
if command -v rg >/dev/null 2>&1; then
  while IFS= read -r image; do
    [[ -n "$image" ]] && images+=("$image")
  done < <(rg -n "^[[:space:]]*image:[[:space:]]*" "$COMPOSE_FILE" \
    | sed -E 's/^.*image:[[:space:]]*//' \
    | sed -E 's/[[:space:]]+#.*$//' \
    | sed -E 's/^"(.*)"$/\1/' \
    | sort -u)
else
  while IFS= read -r image; do
    [[ -n "$image" ]] && images+=("$image")
  done < <(grep -nE "^[[:space:]]*image:[[:space:]]*" "$COMPOSE_FILE" \
    | sed -E 's/^.*image:[[:space:]]*//' \
    | sed -E 's/[[:space:]]+#.*$//' \
    | sed -E 's/^"(.*)"$/\1/' \
    | sort -u)
fi

if (( ${#images[@]} == 0 )); then
  echo "No images found in $COMPOSE_FILE"
  exit 0
fi

classify_image() {
  local image="$1"
  case "$image" in
    minio/minio:*|minio/mc:*|grafana/grafana:*|grafana/loki:*|grafana/tempo:*)
      echo "HIGH|AGPL family|Review network copyleft obligations."
      ;;
    mysql:*)
      echo "HIGH|GPL-2.0 (community)|Review redistribution/commercial obligations."
      ;;
    confluentinc/cp-schema-registry:*|confluentinc/cp-zookeeper:*)
      echo "HIGH|Confluent Community License|Review source-available/commercial restrictions."
      ;;
    docker.elastic.co/elasticsearch/elasticsearch:*)
      echo "HIGH|Elastic License 2.0|Review source-available/restricted terms."
      ;;
    confluentinc/cp-kafka:*)
      echo "MEDIUM|Apache-2.0 + distribution caveats|Verify exact per-version Confluent terms."
      ;;
    *)
      echo "LOW|Likely permissive/other|Verify upstream license and attribution requirements."
      ;;
  esac
}

high_count=0
medium_count=0
low_count=0

printf "%-6s  %-55s  %-36s  %s\n" "Risk" "Image" "Expected License" "Action"
printf "%-6s  %-55s  %-36s  %s\n" "-----" "-----" "----------------" "------"

for image in "${images[@]}"; do
  IFS='|' read -r risk license_hint action <<< "$(classify_image "$image")"
  case "$risk" in
    HIGH) high_count=$((high_count + 1)) ;;
    MEDIUM) medium_count=$((medium_count + 1)) ;;
    LOW) low_count=$((low_count + 1)) ;;
  esac
  printf "%-6s  %-55s  %-36s  %s\n" "$risk" "$image" "$license_hint" "$action"
done

echo
echo "Summary: HIGH=$high_count MEDIUM=$medium_count LOW=$low_count"
echo "Reference: THIRD_PARTY_LICENSES.md"

if [[ "$FAIL_ON_RESTRICTIVE" == "true" && $high_count -gt 0 ]]; then
  echo "FAIL_ON_RESTRICTIVE=true and HIGH-risk images were detected." >&2
  exit 2
fi
