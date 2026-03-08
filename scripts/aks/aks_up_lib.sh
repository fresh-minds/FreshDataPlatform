#!/usr/bin/env bash

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "Missing required command: $1" >&2
    exit 1
  fi
}

is_transient_kube_error() {
  local text="$1"
  [[ "$text" == *"no such host"* ]] || \
  [[ "$text" == *"can't assign requested address"* ]] || \
  [[ "$text" == *"unable to decode an event from the watch stream"* ]] || \
  [[ "$text" == *"TLS handshake timeout"* ]] || \
  [[ "$text" == *"Client.Timeout exceeded"* ]] || \
  [[ "$text" == *"i/o timeout"* ]] || \
  [[ "$text" == *"Unable to connect to the server"* ]]
}

refresh_aks_credentials() {
  log "Refreshing AKS credentials for context '$AKS_CLUSTER_NAME'..."
  az aks get-credentials \
    --resource-group "$AKS_RESOURCE_GROUP" \
    --name "$AKS_CLUSTER_NAME" \
    --overwrite-existing \
    -o none >/dev/null 2>&1 || true
  kubectl config use-context "$AKS_CLUSTER_NAME" >/dev/null 2>&1 || true
}

wait_for_job_complete() {
  local job_name="$1"
  local timeout="${2:-300s}"
  local output
  local attempt
  local succeeded
  local completion_condition

  for attempt in $(seq 1 "$AKS_WAIT_RETRIES"); do
    if output="$(kubectl_ctx -n "$NAMESPACE" wait --for=condition=complete "job/${job_name}" --timeout="$timeout" 2>&1)"; then
      printf '%s\n' "$output"
      return 0
    fi

    printf '%s\n' "$output" >&2

    local image_failure
    image_failure="$(kubectl_ctx -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o jsonpath='{range .items[*]}{.metadata.name}{" "}{.status.containerStatuses[0].state.waiting.reason}{"\n"}{end}' 2>/dev/null | grep -E 'InvalidImageName|ImagePullBackOff|ErrImagePull' || true)"
    if [[ -n "$image_failure" ]]; then
      echo "[aks-up] Job '${job_name}' has image/startup failures:" >&2
      printf '%s\n' "$image_failure" >&2
      kubectl_ctx -n "$NAMESPACE" describe "job/${job_name}" >&2 || true
      kubectl_ctx -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o wide >&2 || true
      return 1
    fi

    if [[ "$attempt" -lt "$AKS_WAIT_RETRIES" ]] && is_transient_kube_error "$output"; then
      log "Transient Kubernetes API error while waiting for job '${job_name}' (attempt ${attempt}/${AKS_WAIT_RETRIES}); retrying in ${AKS_WAIT_RETRY_DELAY_SECONDS}s..."
      refresh_aks_credentials
      sleep "$AKS_WAIT_RETRY_DELAY_SECONDS"
      continue
    fi

    succeeded="$(kubectl_ctx -n "$NAMESPACE" get "job/${job_name}" -o jsonpath='{.status.succeeded}' 2>/dev/null || true)"
    completion_condition="$(kubectl_ctx -n "$NAMESPACE" get "job/${job_name}" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}' 2>/dev/null || true)"
    if [[ "${succeeded:-0}" =~ ^[0-9]+$ ]] && (( succeeded > 0 )); then
      log "Job '${job_name}' reached succeeded state after wait timeout; continuing."
      return 0
    fi
    if [[ "$completion_condition" == "True" ]]; then
      log "Job '${job_name}' reported Complete condition after wait timeout; continuing."
      return 0
    fi

    break
  done

  echo "[aks-up] Job '${job_name}' did not complete within ${timeout}. Dumping diagnostics..." >&2
  kubectl_ctx -n "$NAMESPACE" describe "job/${job_name}" >&2 || true

  local pods
  pods="$(kubectl_ctx -n "$NAMESPACE" get pods -l "job-name=${job_name}" -o name 2>/dev/null || true)"
  if [[ -z "${pods}" ]]; then
    echo "[aks-up] No pods found for job '${job_name}'." >&2
    echo "[aks-up] Recent Job events for '${job_name}':" >&2
    kubectl_ctx -n "$NAMESPACE" get events \
      --sort-by=.lastTimestamp \
      --field-selector "involvedObject.kind=Job,involvedObject.name=${job_name}" >&2 || true
    echo "[aks-up] Recent related Pod events for '${job_name}':" >&2
    kubectl_ctx -n "$NAMESPACE" get events \
      --sort-by=.lastTimestamp \
      | grep -E "${job_name}|job/${job_name}" \
      | tail -n 40 >&2 || true
    return 1
  fi

  while IFS= read -r pod; do
    [[ -z "$pod" ]] && continue
    kubectl_ctx -n "$NAMESPACE" describe "$pod" >&2 || true
    kubectl_ctx -n "$NAMESPACE" logs "$pod" --all-containers=true --tail=200 >&2 || true
    local restart_count
    restart_count="$(kubectl_ctx -n "$NAMESPACE" get "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
    if [[ "${restart_count}" =~ ^[0-9]+$ ]] && (( restart_count > 0 )); then
      kubectl_ctx -n "$NAMESPACE" logs "$pod" --all-containers=true --previous --tail=200 >&2 || true
    fi
  done <<< "$pods"

  return 1
}

wait_for_deployment_in_namespace() {
  local namespace="$1"
  local deployment="$2"
  local timeout="${3:-600s}"
  local output
  local attempt

  if kubectl_ctx -n "$namespace" get deployment "$deployment" >/dev/null 2>&1; then
    for attempt in $(seq 1 "$AKS_WAIT_RETRIES"); do
      if output="$(kubectl_ctx -n "$namespace" rollout status "deployment/${deployment}" --timeout="$timeout" 2>&1)"; then
        printf '%s\n' "$output"
        return 0
      fi

      printf '%s\n' "$output" >&2
      if [[ "$attempt" -lt "$AKS_WAIT_RETRIES" ]] && is_transient_kube_error "$output"; then
        log "Transient Kubernetes API error while waiting for deployment '${deployment}' (attempt ${attempt}/${AKS_WAIT_RETRIES}); retrying in ${AKS_WAIT_RETRY_DELAY_SECONDS}s..."
        refresh_aks_credentials
        sleep "$AKS_WAIT_RETRY_DELAY_SECONDS"
        continue
      fi

      echo "[aks-up] ERROR: rollout for deployment '${deployment}' failed." >&2
      kubectl_ctx -n "$namespace" describe "deployment/${deployment}" >&2 || true
      local selector
      selector="$(kubectl_ctx -n "$namespace" get "deployment/${deployment}" -o go-template='{{range $k, $v := .spec.selector.matchLabels}}{{printf "%s=%s," $k $v}}{{end}}' 2>/dev/null | sed 's/,$//')"
      if [[ -z "$selector" ]]; then
        selector="app.kubernetes.io/name=${deployment}"
      fi
      local pods
      kubectl_ctx -n "$namespace" get pods -l "$selector" -o wide >&2 || true
      pods="$(kubectl_ctx -n "$namespace" get pods -l "$selector" -o name 2>/dev/null || true)"
      if [[ -z "$pods" ]]; then
        echo "[aks-up] No pods found for deployment '${deployment}' using selector '${selector}'." >&2
        kubectl_ctx -n "$namespace" get rs -l "$selector" -o wide >&2 || true
      fi
      if [[ -n "$pods" ]]; then
        while IFS= read -r pod; do
          [[ -z "$pod" ]] && continue
          kubectl_ctx -n "$namespace" describe "$pod" >&2 || true
          kubectl_ctx -n "$namespace" logs "$pod" --all-containers=true --tail=120 >&2 || true
          local restart_count
          restart_count="$(kubectl_ctx -n "$namespace" get "$pod" -o jsonpath='{.status.containerStatuses[0].restartCount}' 2>/dev/null || echo 0)"
          if [[ "${restart_count}" =~ ^[0-9]+$ ]] && (( restart_count > 0 )); then
            kubectl_ctx -n "$namespace" logs "$pod" --all-containers=true --previous --tail=120 >&2 || true
          fi
        done <<< "$pods"
      fi
      return 1
    done
  fi
}

wait_for_deployment() {
  local deployment="$1"
  local timeout="${2:-600s}"
  wait_for_deployment_in_namespace "$NAMESPACE" "$deployment" "$timeout"
}

build_and_push_image() {
  local image="$1"
  local dockerfile="$2"
  local context="$3"
  local label="$4"
  local registry_host
  local -a buildx_args
  local -a docker_build_args
  shift 4

  log "Building and pushing ${label} image '${image}' (linux/amd64)..."

  if [[ "${AKS_USE_CLASSIC_DOCKER_PUSH:-false}" == "true" ]]; then
    docker_build_args=(
      --platform linux/amd64
      --tag "$image"
      --file "$dockerfile"
    )

    if [[ "${AKS_DOCKER_NO_CACHE:-false}" == "true" ]]; then
      docker_build_args+=(--no-cache)
    fi

    docker build \
      "${docker_build_args[@]}" \
      "$@" \
      "$context"

    registry_host="${image%%/*}"
    if [[ -n "${SCW_SECRET_KEY:-}" && "$registry_host" == *"scw.cloud"* ]]; then
      echo "${SCW_SECRET_KEY}" | docker login "$registry_host" -u nologin --password-stdin >/dev/null
    fi

    if ! docker push --platform linux/amd64 "$image"; then
      if [[ -n "${SCW_SECRET_KEY:-}" && "$registry_host" == *"scw.cloud"* ]]; then
        echo "${SCW_SECRET_KEY}" | docker login "$registry_host" -u nologin --password-stdin >/dev/null
      fi
      docker push "$image"
    fi
    return 0
  fi

  buildx_args=(
    --platform linux/amd64
    --tag "$image"
    --file "$dockerfile"
  )

  if [[ "${AKS_DISABLE_BUILDX_ATTESTATIONS:-false}" == "true" ]]; then
    buildx_args+=(--provenance=false --sbom=false)
  fi

  if [[ "${AKS_DOCKER_NO_CACHE:-false}" == "true" ]]; then
    buildx_args+=(--no-cache)
  fi

  docker buildx build \
    "${buildx_args[@]}" \
    "$@" \
    "$context" \
    --push
}

apply_namespaced_manifest() {
  local manifest_path="$1"
  NAMESPACE="$NAMESPACE" yq eval '.metadata.namespace = strenv(NAMESPACE)' "$manifest_path" | kubectl_ctx apply -f -
}
