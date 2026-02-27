# Open Data Platform - Development Commands
.PHONY: install dev-install test lint format format-check run clean help schema-validate schema-drift-check dbt-debug dbt-build-seed dbt-docs-generate dbt-docs-refresh dbt-docs-watch e2e-test test-e2e test-sso qa-test run-job-market-metadata warehouse-metadata-init warehouse-security observability-verify k8s-aks-smoke bootstrap-all bootstrap_all k8s-dev-up k8s-dev-up-full k8s-dev-down k8s-sso-gateway-up k8s-sso-gateway-forward k8s-sso-gateway-forward-stop k8s-aks-up k8s-aks-update-images k8s-aks-down

# Default Python
PYTHON := python3

help:  ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install production dependencies
	$(PYTHON) -m pip install -e .

dev-install:  ## Install development dependencies (lint, typecheck, test tools)
	$(PYTHON) -m pip install -e ".[dev]"

pipeline-install:  ## Install pipeline runtime dependencies (Airflow, Spark, dbt, DataHub)
	$(PYTHON) -m pip install -e ".[dev,pipeline]"

test:  ## Run all tests
	pytest tests/ -v

test-unit:  ## Run unit tests only
	pytest tests/unit/ -v

test-cov:  ## Run tests with coverage report
	pytest tests/ --cov=shared --cov=pipelines --cov-report=html --cov-report=term

lint:  ## Run linter
	ruff check shared/ pipelines/ tests/

format:  ## Format code
	ruff format shared/ pipelines/ tests/

format-check:  ## Check formatting without modifying files
	ruff format --check shared/ pipelines/ tests/

type-check:  ## Run type checker
	mypy shared/ pipelines/

run:  ## Run local pipeline (use PIPELINE=domain.layer_job)
	$(PYTHON) scripts/pipeline/run_local.py --pipeline $(PIPELINE)

run-job-market:  ## Run NL job market pipeline end-to-end (mock data ok)
	$(PYTHON) scripts/pipeline/run_job_market_pipeline.py

run-job-market-metadata:  ## Run NL job market pipeline and ingest metadata into platform_metadata
	$(PYTHON) scripts/pipeline/run_job_market_metadata_pipeline.py

run-job-connectors:  ## Run job aggregator connectors (RSS + sitemap)
	$(PYTHON) scripts/pipeline/run_job_connectors.py

schema-validate:  ## Validate DBML schema definitions
	$(PYTHON) scripts/quality/validate_dbml.py --include-warehouse-structure

schema-drift-check:  ## Detect drift between warehouse DB and schema/warehouse.dbml
	$(PYTHON) scripts/quality/validate_dbml.py --check-warehouse-drift

governance-validate:  ## Validate governance metadata (owner/classification/SLA)
	$(PYTHON) scripts/quality/validate_governance_metadata.py

dq-list:  ## List configured centralized data quality datasets
	$(PYTHON) scripts/quality/run_data_quality.py --list-datasets

dq-check:  ## Run centralized data quality checks for one dataset (use DATASET=domain.table)
	$(PYTHON) scripts/quality/run_data_quality.py --dataset $(DATASET)

dq-check-all:  ## Run centralized data quality checks for all configured datasets
	$(PYTHON) scripts/quality/run_data_quality.py --all

clean:  ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

clean-data:  ## Clean local lakehouse data (prompts for confirmation)
	$(PYTHON) scripts/platform/clean_data.py

dbt-debug:  ## Validate dbt warehouse connection for parallel dbt project
	.venv/bin/dbt debug --project-dir dbt --profiles-dir dbt

dbt-build-seed:  ## Build parallel dbt project using seed data
	.venv/bin/dbt seed --project-dir dbt --profiles-dir dbt --full-refresh
	.venv/bin/dbt run --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
	.venv/bin/dbt snapshot --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'
	.venv/bin/dbt test --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'

dbt-docs-generate:  ## Generate dbt docs artifacts for local lineage UI
	.venv/bin/dbt deps --project-dir dbt --profiles-dir dbt
	.venv/bin/dbt docs generate --project-dir dbt --profiles-dir dbt --vars '{use_seed_data: true}'

dbt-docs-refresh:  ## Regenerate dbt docs and (re)start static dbt docs service
	$(MAKE) dbt-docs-generate
	docker compose up -d dbt-docs

dbt-docs-watch:  ## Watch dbt project changes and auto-regenerate docs + lineage
	docker compose up -d warehouse dbt-docs
	.venv/bin/python scripts/platform/watch_dbt_docs.py --project-dir dbt --profiles-dir dbt

e2e-test:  ## Run end-to-end platform test suite with evidence capture
	./scripts/testing/run_e2e_tests.sh

test-e2e: e2e-test  ## Alias for e2e-test

test-sso:  ## Run SSO E2E suite with Keycloak/browser/API evidence
	./scripts/testing/run_sso_tests.sh

qa-test:  ## Run config-driven QA suites (requires warehouse + dbt artifacts)
	QA_ENV=$${QA_ENV:-test} QA_REQUIRE_SERVICES=true pytest tests/data_quality tests/contracts tests/governance tests/e2e -vv

warehouse-security:  ## Apply warehouse RBAC/RLS/masking baseline
	$(PYTHON) scripts/warehouse/apply_warehouse_security.py

warehouse-metadata-init:  ## Initialize platform metadata schema/tables
	$(PYTHON) scripts/warehouse/init_platform_metadata.py

observability-verify:  ## Verify Docker Compose observability ingestion end-to-end
	./scripts/testing/verify_compose_observability.sh

k8s-aks-smoke:  ## Run in-cluster AKS smoke checks (observability + core platform services)
	./scripts/testing/verify_aks_smoke.sh

bootstrap-all:  ## Start docker stack + seed MinIO/Superset/DataHub/warehouse in one go
	./scripts/platform/bootstrap_all.sh

bootstrap_all: bootstrap-all  ## Alias for bootstrap-all

k8s-dev-up:  ## Start dev-like Kubernetes Phase A stack on a local kind cluster
	./scripts/k8s/k8s_dev_up.sh

k8s-dev-up-full:  ## Start full docker-compose parity stack on a local kind cluster
	./scripts/k8s/k8s_dev_up_full.sh

k8s-dev-down:  ## Tear down dev-like Kubernetes kind cluster
	./scripts/k8s/k8s_dev_down.sh

k8s-sso-gateway-up:  ## Enable ingress + oauth2-proxy shared SSO gateway for local kind
	./scripts/k8s/k8s_enable_sso_gateway.sh

k8s-sso-gateway-forward:  ## Start ingress-nginx port-forward for SSO gateway on localhost:8085
	./scripts/k8s/k8s_port_forward_ingress.sh start

k8s-sso-gateway-forward-stop:  ## Stop ingress-nginx port-forward for SSO gateway
	./scripts/k8s/k8s_port_forward_ingress.sh stop

k8s-aks-up:  ## Provision AKS + deploy stack (Key Vault-backed secrets by default), then run AKS smoke checks (set AKS_SMOKE_AFTER_UP=false to skip)
	./scripts/aks/aks_up.sh
	@if [ "$${AKS_SMOKE_AFTER_UP:-true}" = "true" ]; then \
		echo "Running post-deploy AKS smoke checks (AKS_SMOKE_AFTER_UP=true)"; \
		./scripts/testing/verify_aks_smoke.sh; \
	else \
		echo "Skipping post-deploy AKS smoke checks (AKS_SMOKE_AFTER_UP=$${AKS_SMOKE_AFTER_UP})"; \
	fi

k8s-aks-update-images:  ## Build/push selected app images and patch existing AKS deployments only (no infra/parity reapply)
	./scripts/aks/aks_update_images.sh

k8s-aks-down:  ## Tear down AKS workloads (and optionally infra/Key Vault) created by k8s-aks-up
	./scripts/aks/aks_down.sh

setup:  ## Initial setup (create venv, install deps, copy .env)
	$(PYTHON) -m venv .venv
	@echo "Run 'source .venv/bin/activate' then 'make dev-install'"
	@if [ ! -f .env ]; then cp .env.template .env && echo "Created .env from template - please edit with your secrets"; fi
