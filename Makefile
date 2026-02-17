# Open Data Platform - Development Commands
.PHONY: install dev-install test lint format run clean help schema-validate schema-drift-check dbt-debug dbt-build-seed e2e-test test-e2e qa-test warehouse-security bootstrap-all k8s-dev-up k8s-dev-down k8s-aks-up k8s-aks-down

# Default Python
PYTHON := python3

help:  ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install production dependencies
	$(PYTHON) -m pip install -e .

dev-install:  ## Install development dependencies
	$(PYTHON) -m pip install -e ".[dev]"

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

type-check:  ## Run type checker
	mypy shared/ pipelines/

run:  ## Run local pipeline (use PIPELINE=domain.layer_job)
	$(PYTHON) scripts/run_local.py --pipeline $(PIPELINE)

run-job-market:  ## Run NL job market pipeline end-to-end (mock data ok)
	$(PYTHON) scripts/run_job_market_pipeline.py

run-job-connectors:  ## Run job aggregator connectors (RSS + sitemap)
	$(PYTHON) scripts/run_job_connectors.py

schema-validate:  ## Validate DBML schema definitions
	$(PYTHON) scripts/validate_dbml.py

schema-drift-check:  ## Detect drift between warehouse DB and schema/warehouse.dbml
	$(PYTHON) scripts/validate_dbml.py --check-warehouse-drift

governance-validate:  ## Validate governance metadata (owner/classification/SLA)
	$(PYTHON) scripts/validate_governance_metadata.py

dq-list:  ## List configured centralized data quality datasets
	$(PYTHON) scripts/run_data_quality.py --list-datasets

dq-check:  ## Run centralized data quality checks for one dataset (use DATASET=domain.table)
	$(PYTHON) scripts/run_data_quality.py --dataset $(DATASET)

dq-check-all:  ## Run centralized data quality checks for all configured datasets
	$(PYTHON) scripts/run_data_quality.py --all

clean:  ## Clean build artifacts
	rm -rf build/ dist/ *.egg-info .pytest_cache .mypy_cache .ruff_cache htmlcov/
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

clean-data:  ## Clean local lakehouse data (prompts for confirmation)
	$(PYTHON) scripts/clean_data.py

dbt-debug:  ## Validate dbt warehouse connection for parallel dbt project
	.venv/bin/dbt debug --project-dir dbt_parallel --profiles-dir dbt_parallel

dbt-build-seed:  ## Build parallel dbt project using seed data
	.venv/bin/dbt seed --project-dir dbt_parallel --profiles-dir dbt_parallel --full-refresh
	.venv/bin/dbt run --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'
	.venv/bin/dbt snapshot --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'
	.venv/bin/dbt test --project-dir dbt_parallel --profiles-dir dbt_parallel --vars '{use_seed_data: true}'

e2e-test:  ## Run end-to-end platform test suite with evidence capture
	./scripts/run_e2e_tests.sh

test-e2e: e2e-test  ## Alias for e2e-test

qa-test:  ## Run config-driven QA suites (requires warehouse + dbt artifacts)
	QA_ENV=$${QA_ENV:-test} QA_REQUIRE_SERVICES=true pytest tests/data_quality tests/contracts tests/governance tests/e2e -vv

warehouse-security:  ## Apply warehouse RBAC/RLS/masking baseline
	$(PYTHON) scripts/apply_warehouse_security.py

bootstrap-all:  ## Start docker stack + seed MinIO/Superset/DataHub/warehouse in one go
	./scripts/bootstrap_all.sh

k8s-dev-up:  ## Start dev-like Kubernetes Phase A stack on a local kind cluster
	./scripts/k8s_dev_up.sh

k8s-dev-down:  ## Tear down dev-like Kubernetes kind cluster
	./scripts/k8s_dev_down.sh

k8s-aks-up:  ## Provision AKS + deploy dev-like Kubernetes Phase A stack
	./scripts/aks_up.sh

k8s-aks-down:  ## Tear down AKS workloads (and optionally infra) created by k8s-aks-up
	./scripts/aks_down.sh

setup:  ## Initial setup (create venv, install deps, copy .env)
	$(PYTHON) -m venv .venv
	@echo "Run 'source .venv/bin/activate' then 'make dev-install'"
	@if [ ! -f .env ]; then cp .env.template .env && echo "Created .env from template - please edit with your secrets"; fi
