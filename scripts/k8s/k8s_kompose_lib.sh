#!/usr/bin/env bash
# Shared library for kompose conversion and post-processing.
# Sourced by k8s_dev_up_full.sh (local Kind) and aks_up.sh (Azure AKS).
#
# Required variables before sourcing:
#   ROOT_DIR          – project root
#   KOMPOSE_OUT_DIR   – temp directory for generated manifests
#   KOMPOSE_OVERRIDE  – path to docker-compose.k8s.yml (or empty to skip)
#
# Optional variables:
#   SKIP_MSTEAMS      – "true" to remove prometheus-msteams manifests (arm64)

set -euo pipefail

K8S_SCRIPT_LOG_FORMAT="${K8S_SCRIPT_LOG_FORMAT:-text}"
K8S_SCRIPT_RUN_ID="${K8S_SCRIPT_RUN_ID:-$(date +%Y%m%dT%H%M%S)-$$}"
KOMPOSE_LOG_SOURCE="${KOMPOSE_LOG_SOURCE:-k8s-kompose-lib}"

_kompose_log_timestamp() {
  date -u +"%Y-%m-%dT%H:%M:%SZ"
}

_kompose_log_escape() {
  local value="${1:-}"
  value="${value//\\/\\\\}"
  value="${value//\"/\\\"}"
  value="${value//$'\n'/ }"
  printf '%s' "$value"
}

kompose_log_event() {
  local level="$1"
  local event="$2"
  local outcome="$3"
  local message="${4:-}"
  local timestamp
  timestamp="$(_kompose_log_timestamp)"
  local escaped
  escaped="$(_kompose_log_escape "$message")"

  if [[ "$K8S_SCRIPT_LOG_FORMAT" == "text" || "$K8S_SCRIPT_LOG_FORMAT" == "both" ]]; then
    echo "[$KOMPOSE_LOG_SOURCE][$level][$event][$outcome] $message"
  fi

  if [[ "$K8S_SCRIPT_LOG_FORMAT" == "json" || "$K8S_SCRIPT_LOG_FORMAT" == "both" ]]; then
    printf '{"timestamp":"%s","level":"%s","source":"%s","run_id":"%s","event":"%s","outcome":"%s","namespace":"%s","message":"%s"}\n' \
      "$timestamp" "$level" "$KOMPOSE_LOG_SOURCE" "$K8S_SCRIPT_RUN_ID" "$event" "$outcome" "${NAMESPACE:-}" "$escaped"
  fi
}

_kompose_count_manifests() {
  local pattern="$1"
  find "$KOMPOSE_OUT_DIR" -maxdepth 1 -type f -name "$pattern" | wc -l | tr -d ' '
}

# ---------------------------------------------------------------------------
# kompose_generate – run kompose convert with main + override compose files
# ---------------------------------------------------------------------------
kompose_generate() {
  kompose_log_event "INFO" "kompose_generate" "start" "Running kompose convert into '$KOMPOSE_OUT_DIR'."
  local compose_args=(-f "$ROOT_DIR/docker-compose.yml")
  if [[ -n "${KOMPOSE_OVERRIDE:-}" && -f "$KOMPOSE_OVERRIDE" ]]; then
    compose_args+=(-f "$KOMPOSE_OVERRIDE")
  fi

  kompose convert \
    --volumes hostPath \
    "${compose_args[@]}" \
    -o "$KOMPOSE_OUT_DIR"

  local generated_count
  generated_count="$(_kompose_count_manifests '*.yaml')"
  kompose_log_event "INFO" "kompose_generate" "success" "Generated ${generated_count} manifests."
}

# ---------------------------------------------------------------------------
# kompose_remove_phase_a – remove manifests already managed by Phase A stack
# ---------------------------------------------------------------------------
kompose_remove_phase_a() {
  local before_count
  before_count="$(_kompose_count_manifests '*.yaml')"
  kompose_log_event "INFO" "kompose_remove_phase_a" "start" "Removing manifests already managed by Phase A stack."

  rm -f \
    "$KOMPOSE_OUT_DIR"/airflow-*.yaml \
    "$KOMPOSE_OUT_DIR"/create-buckets-*.yaml \
    "$KOMPOSE_OUT_DIR"/keycloak-*.yaml \
    "$KOMPOSE_OUT_DIR"/minio-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/minio-service.yaml \
    "$KOMPOSE_OUT_DIR"/postgres-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/postgres-service.yaml \
    "$KOMPOSE_OUT_DIR"/warehouse-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/warehouse-service.yaml \
    "$KOMPOSE_OUT_DIR"/datahub-*-setup-*.yaml \
    "$KOMPOSE_OUT_DIR"/datahub-upgrade-*.yaml \
    "$KOMPOSE_OUT_DIR"/airflow-init-*.yaml \
    "$KOMPOSE_OUT_DIR"/dbt-docs-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/dbt-docs-service.yaml

  if [[ "${SKIP_MSTEAMS:-false}" == "true" ]]; then
    rm -f \
      "$KOMPOSE_OUT_DIR"/prometheus-msteams-deployment.yaml \
      "$KOMPOSE_OUT_DIR"/prometheus-msteams-service.yaml
  fi

  local after_count removed_count
  after_count="$(_kompose_count_manifests '*.yaml')"
  removed_count="$((before_count - after_count))"
  kompose_log_event "INFO" "kompose_remove_phase_a" "success" "Removed ${removed_count} manifests; ${after_count} remain."
}

# ---------------------------------------------------------------------------
# kompose_remove_non_essential – remove DataHub, heavy observability, jupyter
#   Used by --minimal deploys to keep only core pipeline + portal + dbt-docs.
# ---------------------------------------------------------------------------
kompose_remove_non_essential() {
  local before_count after_count removed_count
  before_count="$(_kompose_count_manifests '*.yaml')"
  kompose_log_event "INFO" "kompose_remove_non_essential" "start" "Removing non-essential manifests for minimal deploy."

  # Remove DataHub (all components)
  rm -f "$KOMPOSE_OUT_DIR"/datahub-*.yaml

  # Remove heavy observability (keep postgres-exporter-*)
  rm -f \
    "$KOMPOSE_OUT_DIR"/prometheus-deployment.yaml \
    "$KOMPOSE_OUT_DIR"/prometheus-service.yaml \
    "$KOMPOSE_OUT_DIR"/prometheus-msteams-*.yaml \
    "$KOMPOSE_OUT_DIR"/alertmanager-*.yaml \
    "$KOMPOSE_OUT_DIR"/grafana-*.yaml \
    "$KOMPOSE_OUT_DIR"/loki-*.yaml \
    "$KOMPOSE_OUT_DIR"/promtail-*.yaml \
    "$KOMPOSE_OUT_DIR"/tempo-*.yaml \
    "$KOMPOSE_OUT_DIR"/otel-collector-*.yaml \
    "$KOMPOSE_OUT_DIR"/statsd-exporter-*.yaml

  # Remove jupyter
  rm -f "$KOMPOSE_OUT_DIR"/jupyter-*.yaml

  after_count="$(_kompose_count_manifests '*.yaml')"
  removed_count="$((before_count - after_count))"
  kompose_log_event "INFO" "kompose_remove_non_essential" "success" "Removed ${removed_count} manifests; ${after_count} remain."
}

# ---------------------------------------------------------------------------
# kompose_normalise_services – fix service port collisions
# ---------------------------------------------------------------------------
kompose_normalise_services() {
  kompose_log_event "INFO" "kompose_normalise_services" "start" "Normalizing generated Service ports."
  local manifest
  local changed_count=0
  for manifest in "$KOMPOSE_OUT_DIR"/*-service.yaml; do
    [[ -f "$manifest" ]] || continue
    yq -i '(.spec.ports[]? |= (.port = .targetPort))' "$manifest"
    changed_count=$((changed_count + 1))
  done

  # kompose emits both host-mapped and internal OTEL ports; after
  # normalization these collide on 4317/4318. Deduplicate explicitly.
  if [[ -f "$KOMPOSE_OUT_DIR/otel-collector-service.yaml" ]]; then
    yq -i '.spec.ports = [
      {"name":"otel-metrics","port":8889,"targetPort":8889,"protocol":"TCP"},
      {"name":"otlp-grpc","port":4317,"targetPort":4317,"protocol":"TCP"},
      {"name":"otlp-http","port":4318,"targetPort":4318,"protocol":"TCP"}
    ]' "$KOMPOSE_OUT_DIR/otel-collector-service.yaml"
  fi

  # DataHub Kafka advertises the service DNS name and needs to be able to
  # resolve itself before readiness flips true.
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-kafka-service.yaml" ]]; then
    yq -i '.spec.publishNotReadyAddresses = true' "$KOMPOSE_OUT_DIR/datahub-kafka-service.yaml"
  fi

  kompose_log_event "INFO" "kompose_normalise_services" "success" "Normalized ${changed_count} service manifests."
}

# ---------------------------------------------------------------------------
# kompose_fix_deployments – probe fixes, enableServiceLinks, superset args
# ---------------------------------------------------------------------------
kompose_fix_deployments() {
  kompose_log_event "INFO" "kompose_fix_deployments" "start" "Applying deployment probe and env-link safety fixes."
  local manifest
  local changed_count=0
  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue
    # Disable Kubernetes service-link env var injection to avoid
    # collisions like DATAHUB_GMS_PORT=tcp://... overriding app config.
    yq -i '.spec.template.spec.enableServiceLinks = false' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("livenessProbe") and .livenessProbe.exec.command and ((.livenessProbe.exec.command | length) == 1)) | .livenessProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("readinessProbe") and .readinessProbe.exec.command and ((.readinessProbe.exec.command | length) == 1)) | .readinessProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    yq -i '(.spec.template.spec.containers[]? | select(has("startupProbe") and .startupProbe.exec.command and ((.startupProbe.exec.command | length) == 1)) | .startupProbe.exec.command) |= ["sh", "-c", .[0]]' "$manifest"
    changed_count=$((changed_count + 1))
  done

  # Superset needs its bootstrap command preserved as a proper shell array.
  if [[ -f "$KOMPOSE_OUT_DIR/superset-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].args = [
      "sh",
      "-c",
      "pip install --no-cache-dir authlib && superset fab create-admin --username admin --firstname Superset --lastname Admin --email admin@superset.com --password admin || true && superset db upgrade && superset init && /usr/bin/run-server.sh & SERVER_PID=$! && echo [Superset] Waiting for /health... && for i in $(seq 1 60); do curl -sSf http://localhost:8088/health >/dev/null && break || sleep 2; done && python /app/scripts/superset/superset_bootstrap_odp_staffing_demand.py || true && wait $SERVER_PID"
    ]' "$KOMPOSE_OUT_DIR/superset-deployment.yaml"
  fi

  # Alertmanager needs a concrete config file mount in AKS.
  if [[ -f "$KOMPOSE_OUT_DIR/alertmanager-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "alertmanager-config",
        "mountPath": "/etc/alertmanager/alertmanager.yml",
        "subPath": "alertmanager.yml"
      },
      {
        "name": "alertmanager-data",
        "mountPath": "/alertmanager"
      }
    ]' "$KOMPOSE_OUT_DIR/alertmanager-deployment.yaml"

    yq -i '.spec.template.spec.volumes = [
      {
        "name": "alertmanager-config",
        "configMap": {
          "name": "alertmanager-config",
          "items": [
            {
              "key": "alertmanager.yml",
              "path": "alertmanager.yml"
            }
          ]
        }
      },
      {
        "name": "alertmanager-data",
        "emptyDir": {}
      }
    ]' "$KOMPOSE_OUT_DIR/alertmanager-deployment.yaml"
  fi

  # Observability services mount single config files in Compose. In AKS,
  # bind those as ConfigMap file mounts (subPath) instead of emptyDir dirs.
  if [[ -f "$KOMPOSE_OUT_DIR/loki-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "loki-config",
        "mountPath": "/etc/loki/local-config.yaml",
        "subPath": "local-config.yaml"
      },
      {
        "name": "loki-data",
        "mountPath": "/loki"
      }
    ]' "$KOMPOSE_OUT_DIR/loki-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "loki-config",
        "configMap": {
          "name": "loki-config",
          "items": [
            {
              "key": "local-config.yaml",
              "path": "local-config.yaml"
            }
          ]
        }
      },
      {
        "name": "loki-data",
        "emptyDir": {}
      }
    ]' "$KOMPOSE_OUT_DIR/loki-deployment.yaml"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/prometheus-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "prometheus-config",
        "mountPath": "/etc/prometheus/prometheus.yml",
        "subPath": "prometheus.yml"
      },
      {
        "name": "prometheus-config",
        "mountPath": "/etc/prometheus/alerts.yml",
        "subPath": "alerts.yml"
      },
      {
        "name": "prometheus-data",
        "mountPath": "/prometheus"
      }
    ]' "$KOMPOSE_OUT_DIR/prometheus-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "prometheus-config",
        "configMap": {
          "name": "prometheus-config",
          "items": [
            {
              "key": "prometheus.yml",
              "path": "prometheus.yml"
            },
            {
              "key": "alerts.yml",
              "path": "alerts.yml"
            }
          ]
        }
      },
      {
        "name": "prometheus-data",
        "emptyDir": {}
      }
    ]' "$KOMPOSE_OUT_DIR/prometheus-deployment.yaml"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/promtail-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "promtail-config",
        "mountPath": "/etc/promtail/config.yml",
        "subPath": "config.yml"
      }
    ]' "$KOMPOSE_OUT_DIR/promtail-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "promtail-config",
        "configMap": {
          "name": "promtail-config",
          "items": [
            {
              "key": "config.yml",
              "path": "config.yml"
            }
          ]
        }
      }
    ]' "$KOMPOSE_OUT_DIR/promtail-deployment.yaml"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/tempo-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "tempo-config",
        "mountPath": "/etc/tempo.yml",
        "subPath": "tempo.yml"
      },
      {
        "name": "tempo-data",
        "mountPath": "/var/tempo"
      }
    ]' "$KOMPOSE_OUT_DIR/tempo-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "tempo-config",
        "configMap": {
          "name": "tempo-config",
          "items": [
            {
              "key": "tempo.yml",
              "path": "tempo.yml"
            }
          ]
        }
      },
      {
        "name": "tempo-data",
        "emptyDir": {}
      }
    ]' "$KOMPOSE_OUT_DIR/tempo-deployment.yaml"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/otel-collector-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "otel-collector-config",
        "mountPath": "/etc/otel-collector.yml",
        "subPath": "otel-collector.yml"
      }
    ]' "$KOMPOSE_OUT_DIR/otel-collector-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "otel-collector-config",
        "configMap": {
          "name": "otel-collector-config",
          "items": [
            {
              "key": "otel-collector.yml",
              "path": "otel-collector.yml"
            }
          ]
        }
      }
    ]' "$KOMPOSE_OUT_DIR/otel-collector-deployment.yaml"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/grafana-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "grafana-data",
        "mountPath": "/var/lib/grafana"
      },
      {
        "name": "grafana-config",
        "mountPath": "/etc/grafana/provisioning/datasources/datasources.yml",
        "subPath": "datasources.yml"
      },
      {
        "name": "grafana-config",
        "mountPath": "/etc/grafana/provisioning/dashboards/dashboards.yml",
        "subPath": "dashboards.yml"
      },
      {
        "name": "grafana-dashboards",
        "mountPath": "/var/lib/grafana/dashboards"
      }
    ]' "$KOMPOSE_OUT_DIR/grafana-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "grafana-data",
        "emptyDir": {}
      },
      {
        "name": "grafana-config",
        "configMap": {
          "name": "grafana-config",
          "items": [
            {
              "key": "datasources.yml",
              "path": "datasources.yml"
            },
            {
              "key": "dashboards.yml",
              "path": "dashboards.yml"
            }
          ]
        }
      },
      {
        "name": "grafana-dashboards",
        "configMap": {
          "name": "grafana-dashboards"
        }
      }
    ]' "$KOMPOSE_OUT_DIR/grafana-deployment.yaml"
  fi

  # DataHub GMS can require a long cold start (upgrades/index checks).
  # Ensure startup has a dedicated budget and only expose endpoint when healthy.
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml" ]]; then
    local gms_startup_initial_delay_seconds="${DATAHUB_GMS_STARTUP_INITIAL_DELAY_SECONDS:-60}"
    local gms_startup_period_seconds="${DATAHUB_GMS_STARTUP_PERIOD_SECONDS:-10}"
    local gms_startup_timeout_seconds="${DATAHUB_GMS_STARTUP_TIMEOUT_SECONDS:-5}"
    local gms_startup_failure_threshold="${DATAHUB_GMS_STARTUP_FAILURE_THRESHOLD:-240}"
    local gms_readiness_initial_delay_seconds="${DATAHUB_GMS_READINESS_INITIAL_DELAY_SECONDS:-30}"
    local gms_readiness_period_seconds="${DATAHUB_GMS_READINESS_PERIOD_SECONDS:-15}"
    local gms_readiness_timeout_seconds="${DATAHUB_GMS_READINESS_TIMEOUT_SECONDS:-10}"
    local gms_readiness_failure_threshold="${DATAHUB_GMS_READINESS_FAILURE_THRESHOLD:-6}"
    local gms_liveness_initial_delay_seconds="${DATAHUB_GMS_LIVENESS_INITIAL_DELAY_SECONDS:-600}"
    local gms_liveness_period_seconds="${DATAHUB_GMS_LIVENESS_PERIOD_SECONDS:-30}"
    local gms_liveness_timeout_seconds="${DATAHUB_GMS_LIVENESS_TIMEOUT_SECONDS:-10}"
    local gms_liveness_failure_threshold="${DATAHUB_GMS_LIVENESS_FAILURE_THRESHOLD:-10}"

    local gms_cpu_request="${DATAHUB_GMS_CPU_REQUEST:-300m}"
    local gms_memory_request="${DATAHUB_GMS_MEMORY_REQUEST:-1Gi}"
    local gms_cpu_limit="${DATAHUB_GMS_CPU_LIMIT:-1500m}"
    local gms_memory_limit="${DATAHUB_GMS_MEMORY_LIMIT:-2Gi}"

    # Use a socket startup probe so cold-start bootstrap doesn't require full /health.
    yq -i ".spec.template.spec.containers[0].startupProbe = {
      \"tcpSocket\": {
        \"port\": 8080
      },
      \"initialDelaySeconds\": ${gms_startup_initial_delay_seconds},
      \"periodSeconds\": ${gms_startup_period_seconds},
      \"timeoutSeconds\": ${gms_startup_timeout_seconds},
      \"failureThreshold\": ${gms_startup_failure_threshold},
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].readinessProbe = {
      \"exec\": {
        \"command\": [\"sh\", \"-c\", \"curl -sS --fail http://localhost:8080/health\"]
      },
      \"initialDelaySeconds\": ${gms_readiness_initial_delay_seconds},
      \"periodSeconds\": ${gms_readiness_period_seconds},
      \"timeoutSeconds\": ${gms_readiness_timeout_seconds},
      \"failureThreshold\": ${gms_readiness_failure_threshold},
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].livenessProbe = {
      \"exec\": {
        \"command\": [\"sh\", \"-c\", \"curl -sS --fail http://localhost:8080/health\"]
      },
      \"initialDelaySeconds\": ${gms_liveness_initial_delay_seconds},
      \"periodSeconds\": ${gms_liveness_period_seconds},
      \"timeoutSeconds\": ${gms_liveness_timeout_seconds},
      \"failureThreshold\": ${gms_liveness_failure_threshold},
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].resources = {
      \"requests\": {
        \"cpu\": \"${gms_cpu_request}\",
        \"memory\": \"${gms_memory_request}\"
      },
      \"limits\": {
        \"cpu\": \"${gms_cpu_limit}\",
        \"memory\": \"${gms_memory_limit}\"
      }
    }" "$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
  fi

  # DataHub MySQL can take time to initialize system tables on ephemeral disks.
  # Use startup/readiness gating and avoid aggressive early liveness restarts.
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-elasticsearch-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].startupProbe = {
      "httpGet": {
        "path": "/",
        "port": 9200,
        "scheme": "HTTP"
      },
      "initialDelaySeconds": 20,
      "periodSeconds": 10,
      "timeoutSeconds": 5,
      "failureThreshold": 60,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-elasticsearch-deployment.yaml"
    yq -i '.spec.template.spec.containers[0].readinessProbe = {
      "httpGet": {
        "path": "/",
        "port": 9200,
        "scheme": "HTTP"
      },
      "initialDelaySeconds": 10,
      "periodSeconds": 10,
      "timeoutSeconds": 5,
      "failureThreshold": 12,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-elasticsearch-deployment.yaml"
    yq -i '.spec.template.spec.containers[0].livenessProbe = {
      "httpGet": {
        "path": "/",
        "port": 9200,
        "scheme": "HTTP"
      },
      "initialDelaySeconds": 120,
      "periodSeconds": 15,
      "timeoutSeconds": 5,
      "failureThreshold": 10,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-elasticsearch-deployment.yaml"
  fi

  # DataHub MySQL can take time to initialize system tables on ephemeral disks.
  # Use startup/readiness gating and avoid aggressive early liveness restarts.
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-mysql-deployment.yaml" ]]; then
    yq -i '.spec.template.spec.containers[0].startupProbe = {
      "exec": {
        "command": ["sh", "-c", "mysqladmin ping -h 127.0.0.1 -uroot --password=$MYSQL_ROOT_PASSWORD"]
      },
      "initialDelaySeconds": 30,
      "periodSeconds": 10,
      "timeoutSeconds": 5,
      "failureThreshold": 60,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-mysql-deployment.yaml"
    yq -i '.spec.template.spec.containers[0].readinessProbe = {
      "exec": {
        "command": ["sh", "-c", "mysqladmin ping -h 127.0.0.1 -uroot --password=$MYSQL_ROOT_PASSWORD"]
      },
      "initialDelaySeconds": 20,
      "periodSeconds": 10,
      "timeoutSeconds": 5,
      "failureThreshold": 12,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-mysql-deployment.yaml"
    yq -i '.spec.template.spec.containers[0].livenessProbe = {
      "exec": {
        "command": ["sh", "-c", "mysqladmin ping -h 127.0.0.1 -uroot --password=$MYSQL_ROOT_PASSWORD"]
      },
      "initialDelaySeconds": 120,
      "periodSeconds": 15,
      "timeoutSeconds": 5,
      "failureThreshold": 10,
      "successThreshold": 1
    }' "$KOMPOSE_OUT_DIR/datahub-mysql-deployment.yaml"
  fi

  # DataHub Kafka can accept TCP before broker metadata is fully available.
  # Gate startup/readiness on broker API metadata checks to avoid setup-job races.
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml" ]]; then
    local kafka_startup_initial_delay_seconds=40
    local kafka_startup_timeout_seconds=10
    local kafka_startup_failure_threshold=60
    local kafka_readiness_initial_delay_seconds=20
    local kafka_readiness_timeout_seconds=10
    local kafka_readiness_failure_threshold=24
    local kafka_liveness_initial_delay_seconds=60

    if [[ "${KOMPOSE_LOG_SOURCE:-}" == "aks-up" ]]; then
      kafka_startup_initial_delay_seconds=60
      kafka_startup_timeout_seconds=30
      kafka_startup_failure_threshold=90
      kafka_readiness_initial_delay_seconds=30
      kafka_readiness_timeout_seconds=30
      kafka_readiness_failure_threshold=36
      kafka_liveness_initial_delay_seconds=90
    fi

    yq -i '.spec.strategy = {
      "type": "Recreate"
    }' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[] | select(.name == "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP") | .value) = "PLAINTEXT:PLAINTEXT"' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[] | select(.name == "KAFKA_ADVERTISED_LISTENERS") | .value) = "PLAINTEXT://datahub-kafka:29092"' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[] | select(.name == "KAFKA_LISTENERS") | .value) = "PLAINTEXT://0.0.0.0:29092"' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[] | select(.name == "KAFKA_INTER_BROKER_LISTENER_NAME") | .value) = "PLAINTEXT"' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].startupProbe = {
      \"exec\": {
        \"command\": [\"sh\", \"-c\", \"kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1\"]
      },
      \"initialDelaySeconds\": ${kafka_startup_initial_delay_seconds},
      \"periodSeconds\": 10,
      \"timeoutSeconds\": ${kafka_startup_timeout_seconds},
      \"failureThreshold\": ${kafka_startup_failure_threshold},
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].readinessProbe = {
      \"exec\": {
        \"command\": [\"sh\", \"-c\", \"kafka-broker-api-versions --bootstrap-server localhost:29092 >/dev/null 2>&1\"]
      },
      \"initialDelaySeconds\": ${kafka_readiness_initial_delay_seconds},
      \"periodSeconds\": 10,
      \"timeoutSeconds\": ${kafka_readiness_timeout_seconds},
      \"failureThreshold\": ${kafka_readiness_failure_threshold},
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    yq -i ".spec.template.spec.containers[0].livenessProbe = {
      \"tcpSocket\": {
        \"port\": 29092
      },
      \"initialDelaySeconds\": ${kafka_liveness_initial_delay_seconds},
      \"periodSeconds\": 10,
      \"timeoutSeconds\": 5,
      \"failureThreshold\": 10,
      \"successThreshold\": 1
    }" "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"

    if [[ "${KOMPOSE_LOG_SOURCE:-}" == "aks-up" ]]; then
      # AKS needs explicit Kafka resources to avoid memory-pressure evictions.
      yq -i '.spec.template.spec.containers[0].resources = {
        "requests": {
          "cpu": "300m",
          "memory": "1200Mi"
        },
        "limits": {
          "cpu": "1000m",
          "memory": "2048Mi"
        }
      }' "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml"
    fi
  fi

  kompose_log_event "INFO" "kompose_fix_deployments" "success" "Updated ${changed_count} deployment manifests."
}

# ---------------------------------------------------------------------------
# kompose_hold_datahub – move GMS/frontend aside so they deploy after jobs
# ---------------------------------------------------------------------------
kompose_hold_datahub() {
  kompose_log_event "INFO" "kompose_hold_datahub" "start" "Holding DataHub manifests until setup jobs complete."
  local gms="$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
  local fe="$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml"
  [[ -f "$gms" ]] && mv "$gms" "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold"
  [[ -f "$fe" ]] && mv "$fe" "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold"
  kompose_log_event "INFO" "kompose_hold_datahub" "success" "DataHub hold manifests prepared."
}

# ---------------------------------------------------------------------------
# kompose_restore_datahub – bring held manifests back
# ---------------------------------------------------------------------------
kompose_restore_datahub() {
  kompose_log_event "INFO" "kompose_restore_datahub" "start" "Restoring held DataHub manifests."
  local gms="$KOMPOSE_OUT_DIR/datahub-gms-deployment.yaml"
  local fe="$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml"
  [[ -f "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold" ]] && mv "$KOMPOSE_OUT_DIR/.datahub-gms-deployment.hold" "$gms"
  [[ -f "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold" ]] && mv "$KOMPOSE_OUT_DIR/.datahub-frontend-deployment.hold" "$fe"
  kompose_log_event "INFO" "kompose_restore_datahub" "success" "DataHub manifests restored."
}

# ---------------------------------------------------------------------------
# kompose_postprocess_local – Kind-specific volume rewrites
#   Required: KIND_MOUNT_PATH
# ---------------------------------------------------------------------------
kompose_postprocess_local() {
  kompose_log_event "INFO" "kompose_postprocess_local" "start" "Applying Kind-specific manifest rewrites."
  local manifest

  # Rewrite host paths from the actual repo directory to the Kind mount path
  while IFS= read -r -d '' manifest; do
    perl -0pi -e "s|\Q$ROOT_DIR\E|$KIND_MOUNT_PATH|g" "$manifest"
  done < <(find "$KOMPOSE_OUT_DIR" -type f -name '*.yaml' -print0)

  # Convert synthetic root hostPath volumes (project root) to emptyDir
  export KIND_MOUNT_PATH
  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue
    yq -i '(.spec.template.spec.volumes[]? | select(has("hostPath") and .hostPath.path == strenv(KIND_MOUNT_PATH))) |= {"name": .name, "emptyDir": {}}' "$manifest"
  done

  # Remove development bind mounts for images that run from container FS
  if [[ -f "$KOMPOSE_OUT_DIR/portal-deployment.yaml" ]]; then
    yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/portal-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml" ]]; then
    yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[]? | select(.name == "JUPYTER_WORKDIR").value) = "/workspace"' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
  fi

  kompose_log_event "INFO" "kompose_postprocess_local" "success" "Completed Kind-specific post-processing."
}

# ---------------------------------------------------------------------------
# kompose_postprocess_aks – AKS-specific: remove hostPaths, replace images
#   Required: ACR_NAME, FRONTEND_IMAGE, PORTAL_API_IMAGE, MINIO_SSO_BRIDGE_IMAGE, JUPYTER_IMAGE
# ---------------------------------------------------------------------------
kompose_postprocess_aks() {
  kompose_log_event "INFO" "kompose_postprocess_aks" "start" "Applying AKS-specific manifest rewrites."
  local manifest
  local keycloak_public_url="https://keycloak.${FRONTEND_DOMAIN}"
  local portal_api_public_url="https://portal-api.${FRONTEND_DOMAIN}"
  local dbt_docs_public_url="https://dbt-docs.${FRONTEND_DOMAIN}"
  local minio_public_url="https://minio.${FRONTEND_DOMAIN}"
  local grafana_public_url="https://grafana.${FRONTEND_DOMAIN}"
  local superset_auth_url="${keycloak_public_url}/realms/odp/protocol/openid-connect/auth"
  local datahub_public_url="https://datahub.${FRONTEND_DOMAIN}"
  local keycloak_discovery_url="${keycloak_public_url}/realms/odp/.well-known/openid-configuration"
  local superset_config_file="$ROOT_DIR/scripts/superset/superset_config.py"
  local superset_bootstrap_file="$ROOT_DIR/scripts/superset/superset_bootstrap_odp_staffing_demand.py"

  set_or_add_env_var() {
    local target_manifest="$1"
    local env_name="$2"
    local env_value="$3"
    ENV_NAME="$env_name" ENV_VALUE="$env_value" yq -i '
      .spec.template.spec.containers[0].env = (
        (.spec.template.spec.containers[0].env // [])
        | map(select(.name != strenv(ENV_NAME)))
        + [{"name": strenv(ENV_NAME), "value": strenv(ENV_VALUE)}]
      )
    ' "$target_manifest"
  }

  set_or_add_env_vars() {
    local target_manifest="$1"
    shift
    while [[ "$#" -ge 2 ]]; do
      set_or_add_env_var "$target_manifest" "$1" "$2"
      shift 2
    done
  }

  set_or_add_env_var_from_secret() {
    local target_manifest="$1"
    local env_name="$2"
    local secret_name="$3"
    local secret_key="$4"
    ENV_NAME="$env_name" SECRET_NAME="$secret_name" SECRET_KEY="$secret_key" yq -i '
      .spec.template.spec.containers[0].env = (
        (.spec.template.spec.containers[0].env // [])
        | map(select(.name != strenv(ENV_NAME)))
        + [{
            "name": strenv(ENV_NAME),
            "valueFrom": {
              "secretKeyRef": {
                "name": strenv(SECRET_NAME),
                "key": strenv(SECRET_KEY)
              }
            }
          }]
      )
    ' "$target_manifest"
  }

  set_or_add_env_var_from_secret_optional() {
    local target_manifest="$1"
    local env_name="$2"
    local secret_name="$3"
    local secret_key="$4"
    ENV_NAME="$env_name" SECRET_NAME="$secret_name" SECRET_KEY="$secret_key" yq -i '
      .spec.template.spec.containers[0].env = (
        (.spec.template.spec.containers[0].env // [])
        | map(select(.name != strenv(ENV_NAME)))
        + [{
            "name": strenv(ENV_NAME),
            "valueFrom": {
              "secretKeyRef": {
                "name": strenv(SECRET_NAME),
                "key": strenv(SECRET_KEY),
                "optional": true
              }
            }
          }]
      )
    ' "$target_manifest"
  }

  set_or_add_env_vars_from_secret() {
    local target_manifest="$1"
    local secret_name="$2"
    shift 2

    local env_name
    for env_name in "$@"; do
      set_or_add_env_var_from_secret "$target_manifest" "$env_name" "$secret_name" "$env_name"
    done
  }

  set_or_add_env_vars_from_secret_optional() {
    local target_manifest="$1"
    local secret_name="$2"
    shift 2

    local env_name
    for env_name in "$@"; do
      set_or_add_env_var_from_secret_optional "$target_manifest" "$env_name" "$secret_name" "$env_name"
    done
  }

  render_configmap_from_file() {
    local configmap_name="$1"
    local key_name="$2"
    local source_file="$3"
    local output_file="$4"
    local target_namespace="${NAMESPACE:-odp-dev}"

    [[ -f "$source_file" ]] || return 0

    {
      cat <<EOF
apiVersion: v1
kind: ConfigMap
metadata:
  name: ${configmap_name}
  namespace: ${target_namespace}
data:
  ${key_name}: |-
EOF
      sed 's/^/    /' "$source_file"
    } > "$output_file"
  }

  render_configmap_from_file "superset-config" "superset_config.py" "$superset_config_file" "$KOMPOSE_OUT_DIR/superset-config-configmap.yaml"
  render_configmap_from_file "superset-bootstrap" "superset_bootstrap_odp_staffing_demand.py" "$superset_bootstrap_file" "$KOMPOSE_OUT_DIR/superset-bootstrap-configmap.yaml"

  for manifest in "$KOMPOSE_OUT_DIR"/*-deployment.yaml; do
    [[ -f "$manifest" ]] || continue

    # Replace all hostPath volumes with emptyDir (AKS has no host mounts).
    # Stateful services (databases, MinIO, etc.) get PVC via Phase A manifests.
    yq -i '(.spec.template.spec.volumes[]? | select(has("hostPath"))) |= {"name": .name, "emptyDir": {}}' "$manifest"
  done

  # Remove development bind mounts for images that run from container FS
  for svc in portal portal-api jupyter minio-sso-bridge superset; do
    if [[ -f "$KOMPOSE_OUT_DIR/${svc}-deployment.yaml" ]]; then
      yq -i 'del(.spec.template.spec.containers[]?.volumeMounts) | del(.spec.template.spec.volumes)' "$KOMPOSE_OUT_DIR/${svc}-deployment.yaml"
    fi
  done

  # Replace locally-built image references with ACR images
  if [[ -f "$KOMPOSE_OUT_DIR/portal-deployment.yaml" && -n "${FRONTEND_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${FRONTEND_IMAGE}\"" "$KOMPOSE_OUT_DIR/portal-deployment.yaml"
    yq -i '.spec.template.spec.containers[0].ports = [{"containerPort": 80, "protocol": "TCP"}]' "$KOMPOSE_OUT_DIR/portal-deployment.yaml"
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/portal-deployment.yaml" \
      "VITE_KEYCLOAK_URL" "$keycloak_public_url" \
      "VITE_PORTAL_API_URL" "$portal_api_public_url" \
      "VITE_DBT_DOCS_URL" "$dbt_docs_public_url" \
      "VITE_KEYCLOAK_REALM" "odp" \
      "VITE_KEYCLOAK_CLIENT_ID" "portal"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/portal-service.yaml" ]]; then
    yq -i '.spec.ports = [{"name":"http","port": 80, "targetPort": 80, "protocol": "TCP"}]' "$KOMPOSE_OUT_DIR/portal-service.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml" && -n "${PORTAL_API_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${PORTAL_API_IMAGE}\"" "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml"
    set_or_add_env_var "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml" "PORTAL_CORS_ORIGINS" "https://${FRONTEND_DOMAIN},https://www.${FRONTEND_DOMAIN}"
    # Primary path: use AZURE_EXISTING_* values from odp-env.
    # Backward compatibility: still accept AZURE_FOUNDRY_* when present.
    set_or_add_env_vars_from_secret "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml" "odp-env" \
      "AZURE_EXISTING_AIPROJECT_ENDPOINT" \
      "AZURE_EXISTING_AGENT_ID"
    set_or_add_env_vars_from_secret_optional "$KOMPOSE_OUT_DIR/portal-api-deployment.yaml" "odp-env" \
      "AZURE_EXISTING_AGENT_NAME" \
      "AZURE_EXISTING_AIPROJECT_RESOURCE_ID" \
      "AZURE_EXISTING_RESOURCE_ID" \
      "AZURE_FOUNDRY_AGENT_ENDPOINT" \
      "AZURE_FOUNDRY_AGENT_ID" \
      "AZURE_FOUNDRY_AGENT_NAME" \
      "AZURE_FOUNDRY_API_KEY" \
      "AZURE_TENANT_ID" \
      "AZURE_CLIENT_ID" \
      "AZURE_CLIENT_SECRET"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/minio-sso-bridge-deployment.yaml" && -n "${MINIO_SSO_BRIDGE_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${MINIO_SSO_BRIDGE_IMAGE}\"" "$KOMPOSE_OUT_DIR/minio-sso-bridge-deployment.yaml"
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/minio-sso-bridge-deployment.yaml" \
      "KEYCLOAK_BROWSER_BASE_URL" "$keycloak_public_url" \
      "BRIDGE_BASE_URL" "$minio_public_url" \
      "MINIO_CONSOLE_PUBLIC_URL" "$minio_public_url"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml" && -n "${JUPYTER_IMAGE:-}" ]]; then
    yq -i ".spec.template.spec.containers[0].image = \"${JUPYTER_IMAGE}\"" "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
    yq -i '(.spec.template.spec.containers[0].env[]? | select(.name == "JUPYTER_WORKDIR").value) = "/workspace"' "$KOMPOSE_OUT_DIR/jupyter-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/grafana-deployment.yaml" ]]; then
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/grafana-deployment.yaml" \
      "GF_AUTH_GENERIC_OAUTH_AUTH_URL" "$superset_auth_url" \
      "GF_SERVER_ROOT_URL" "$grafana_public_url"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/superset-deployment.yaml" ]]; then
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/superset-deployment.yaml" \
      "KEYCLOAK_OIDC_SUPERSET_BROWSER_AUTHORIZE_URL" "$superset_auth_url" \
      "SUPERSET_CONFIG_PATH" "/app/pythonpath/superset_config.py"
    yq -i '.spec.template.spec.containers[0].volumeMounts = [
      {
        "name": "superset-config",
        "mountPath": "/app/pythonpath/superset_config.py",
        "subPath": "superset_config.py"
      },
      {
        "name": "superset-bootstrap",
        "mountPath": "/app/scripts/superset/superset_bootstrap_odp_staffing_demand.py",
        "subPath": "superset_bootstrap_odp_staffing_demand.py"
      }
    ]' "$KOMPOSE_OUT_DIR/superset-deployment.yaml"
    yq -i '.spec.template.spec.volumes = [
      {
        "name": "superset-config",
        "configMap": {
          "name": "superset-config",
          "items": [
            {
              "key": "superset_config.py",
              "path": "superset_config.py"
            }
          ]
        }
      },
      {
        "name": "superset-bootstrap",
        "configMap": {
          "name": "superset-bootstrap",
          "items": [
            {
              "key": "superset_bootstrap_odp_staffing_demand.py",
              "path": "superset_bootstrap_odp_staffing_demand.py"
            }
          ]
        }
      }
    ]' "$KOMPOSE_OUT_DIR/superset-deployment.yaml"
  fi
  if [[ -f "$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml" ]]; then
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/datahub-frontend-deployment.yaml" \
      "AUTH_OIDC_BASE_URL" "$datahub_public_url" \
      "AUTH_OIDC_DISCOVERY_URI" "$keycloak_discovery_url"
  fi

  if [[ -f "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml" ]]; then
    set_or_add_env_vars "$KOMPOSE_OUT_DIR/datahub-kafka-deployment.yaml" \
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP" "PLAINTEXT:PLAINTEXT" \
      "KAFKA_ADVERTISED_LISTENERS" "PLAINTEXT://datahub-kafka:29092" \
      "KAFKA_LISTENERS" "PLAINTEXT://0.0.0.0:29092" \
      "KAFKA_INTER_BROKER_LISTENER_NAME" "PLAINTEXT"
  fi

  kompose_log_event "INFO" "kompose_postprocess_aks" "success" "Completed AKS-specific post-processing."
}

# ---------------------------------------------------------------------------
# se_postprocess_aks – backward-compatible alias for older aks_up.sh hooks
# ---------------------------------------------------------------------------
se_postprocess_aks() {
  kompose_postprocess_aks "$@"
}

# ---------------------------------------------------------------------------
# EXTENDED_DEPLOYMENTS – common list of deployments to wait for after apply
# ---------------------------------------------------------------------------
EXTENDED_DEPLOYMENTS=(
  minio-sso-bridge
  superset-db
  superset
  datahub-gms
  datahub-frontend
  portal
  portal-api
  jupyter
  alertmanager
  prometheus
  loki
  tempo
  otel-collector
  grafana
  statsd-exporter
  postgres-exporter-airflow
  postgres-exporter-warehouse
  promtail
)

DATAHUB_DEPS=(
  datahub-mysql
  datahub-elasticsearch
  datahub-zookeeper
  datahub-kafka
  datahub-schema-registry
)

DATAHUB_SETUP_JOBS=(
  datahub-mysql-setup
  datahub-elasticsearch-setup
  datahub-kafka-setup
  datahub-upgrade
)

# Deployments waited on in --minimal mode (no DataHub, no heavy observability, no jupyter)
MINIMAL_DEPLOYMENTS=(
  minio-sso-bridge
  superset-db
  superset
  portal
  portal-api
  postgres-exporter-airflow
  postgres-exporter-warehouse
)
