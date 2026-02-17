# Open Data Platform

A data platform with local development support and Microsoft Fabric compatibility.

## Quick Start

```bash
# 1. Create and activate virtual environment
python3 -m venv .venv
source .venv/bin/activate  # macOS/Linux

# 2. Install dependencies
make dev-install

# 3. Configure environment
cp .env.template .env
# Edit .env with required local stack secrets + connector credentials

# 4. Run a pipeline locally
make run PIPELINE=domain.layer_job
```

## Project Structure

```
ai_trial/
├── shared/          # Shared library (Fabric mocks, config, clients)
├── pipelines/       # Portable pipeline logic
├── notebooks/       # Fabric notebook wrappers
├── scripts/         # Local utilities
├── tests/           # Test suite
└── data/            # Local lakehouse data
```

## Development Commands

```bash
make help         # Show all commands
make test         # Run tests
make lint         # Check code style
make format       # Auto-format code
make qa-test      # Run config-driven data platform QA suites
make test-e2e     # Run full E2E QA suite + artifact capture
make dq-list      # List centralized data quality suites
make dq-check DATASET=job_market_nl.job_market_snapshot  # Run one DQ suite
make dbt-debug    # Validate dbt connection
make dbt-build-seed  # Build parallel dbt transformations with seed data
```

## E2E Data Platform QA

The repository includes a config-driven E2E testing framework for:
- data correctness and quality
- schema/contracts
- pipeline idempotency/incremental behavior
- governance controls (metadata, lineage, PII, RBAC, retention, auditability)

Documentation: `docs/e2e_data_platform_testing.md`

## Job Connector Framework (RSS + Sitemap)

Connectors live in `shared/job_connectors/` and are run via `scripts/run_job_connectors.py`.

Run locally:

```bash
# Configure:
# - JOB_CONNECTORS_ALLOWLIST_HOSTS (required by default)
# - CONNECTOR_RSS_FEED_URLS and/or CONNECTOR_SITEMAP_URLS
python scripts/run_job_connectors.py --list
python scripts/run_job_connectors.py --dry-run
python scripts/run_job_connectors.py --output ./data/job_connectors/jobs.jsonl
python scripts/run_job_connectors.py --connector rss --dry-run
```

Add a new connector:

1. Implement `shared/job_connectors/connectors/base.py` interface.
2. Register it in `shared/job_connectors/registry.py`.
3. Add deterministic parsing tests with fixtures in `tests/fixtures/job_connectors/<connector>/` (no live network calls in tests).

Compliance checklist:

- Respect `robots.txt` (enabled by default; strict mode configurable via `JOB_CONNECTORS_ROBOTS_STRICT`).
- Do not bypass logins, CAPTCHAs, paywalls, or anti-bot controls.
- Only crawl hosts you are allowed to crawl (enforced by default via `JOB_CONNECTORS_ALLOWLIST_HOSTS`).
- Store only what is needed for aggregation; descriptions are PII-redacted by default (`JOB_CONNECTORS_REDACT_PII=true`).
- Raw payload persistence is off by default; if enabled, treat raw captures as sensitive and use retention (`JOB_CONNECTORS_PERSIST_RAW`, `JOB_CONNECTORS_RAW_RETENTION_DAYS`).

## Observability (OSS)

The docker compose file includes an OSS observability stack:
- Prometheus (metrics) on port 9090
- Grafana (dashboards) on port 3001
- Loki (logs) on port 3100
- Tempo (traces) on port 3200
- Alertmanager (alerts) on port 9093

Minimal startup:
```bash
docker compose up -d prometheus alertmanager prometheus-msteams grafana loki promtail tempo otel-collector statsd-exporter postgres-exporter-airflow postgres-exporter-warehouse
```

Set these in `.env` before starting:
- `GRAFANA_ADMIN_PASSWORD`
- `ALERT_TEAMS_WEBHOOK_URL`
- `OTEL_EXPORTER_OTLP_ENDPOINT` (default `http://localhost:4320` for local scripts)

If you run pipelines inside Docker containers, use:
- `OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318`

## SSO (Keycloak)

The local stack can use Keycloak-based SSO for Airflow, DataHub, and MinIO.

Prereqs:
- Add a hosts entry so the browser can resolve the Keycloak hostname used by the containers:
  - `127.0.0.1 keycloak`
- Set the Keycloak and client secrets in `.env` (see the SSO section in `.env.template`).

Start Keycloak with the stack:
```bash
docker compose up -d keycloak
```

Keycloak admin console:
- `http://keycloak:8090` (admin user + password from `.env`)

Default realm user (for SSO logins):
- Username: `odp-admin`
- Password: `KEYCLOAK_DEFAULT_USER_PASSWORD` from `.env`

## Parallel dbt setup

A full dbt-based parallel transformation stack now lives in `dbt_parallel/`.

Use this to validate or run SQL-native transformations alongside the existing PySpark implementation.

## Kubernetes (Dev-Like First Iteration)

You can run a local Kubernetes Phase A stack (Airflow + metadata DB, warehouse DB, MinIO) on kind:

```bash
make k8s-dev-up
```

See `k8s/README.md` for details and access commands.

You can also provision AKS and deploy the same dev-like stack:

```bash
make k8s-aks-up
```

To tear it down again (workloads + ingress/cert-manager; optional infra flags inside the script):

```bash
make k8s-aks-down
```

Optional environment overrides for AKS deployment:
- `AKS_RESOURCE_GROUP` (default: `ai-trial-rg`)
- `AKS_CLUSTER_NAME` (default: `ai-trial-aks`)
- `AKS_LOCATION` (default: `westeurope`)
- `AKS_NODE_COUNT` (default: `1`)
- `AKS_NODE_VM_SIZE` (default: `Standard_B2s`)
- `ACR_NAME` (default: derived from subscription id)
- `NAMESPACE` (default: `odp-dev`)
- `AKS_FORCE_ATTACH_ACR` (default: `false`; set `true` to force re-attach ACR on existing clusters)
- `FRONTEND_DOMAIN` (default: `eu-sovereigndataplatform.com`)
- `DNS_RESOURCE_GROUP` (default: `AKS_RESOURCE_GROUP`)
- `LETSENCRYPT_EMAIL` (default: `karel.goense@freshminds.nl`)

Example:

```bash
AKS_RESOURCE_GROUP=odp-rg AKS_CLUSTER_NAME=odp-aks AKS_LOCATION=westeurope make k8s-aks-up
```

For public frontend exposure on AKS with TLS/custom domain, use nginx ingress + cert-manager and the manifests in:
- `k8s/aks/frontend.yaml`
- `k8s/aks/cert-issuer-letsencrypt-prod.yaml`
- `k8s/aks/frontend-ingress.yaml`

`make k8s-aks-up` also installs ingress-nginx + cert-manager, configures Azure DNS for `FRONTEND_DOMAIN`, and waits for `frontend-tls` to become Ready.
It publishes:
- `https://FRONTEND_DOMAIN` (frontend app)
- `https://airflow.FRONTEND_DOMAIN` (Airflow UI)
- `https://minio.FRONTEND_DOMAIN` (MinIO Console)
- `https://minio-api.FRONTEND_DOMAIN` (MinIO API)

## Microsoft Fabric Deployment

The `shared/` and `pipelines/` packages can be deployed to Fabric via:
1. Fabric Environment with git integration
2. Upload as wheel package
3. Include in notebook resources
