# CI/CD Runbooks

## Run CI Locally
1. Create a venv and install dev deps:
   - `python -m venv .venv`
   - `source .venv/bin/activate`
   - `pip install -e ".[dev]"`
2. Lint and format checks:
   - `make lint`
   - `make format-check`
3. Type check:
   - `make type-check`
4. Unit tests + coverage:
   - `pytest tests/ -m "not e2e and not pipeline_e2e" --junitxml=reports/junit.xml --cov=shared --cov=pipelines --cov-report=xml:reports/coverage.xml`

## Run CI Frontend Checks
1. `cd frontend`
2. `npm ci`
3. `npm run lint`
4. `npm run format:check`
5. `npm run build`

## Cut a Release
1. Use Conventional Commits on `main`.
2. Release Please opens a release PR automatically; merge it.
3. On merge, the `Release` workflow creates a GitHub Release, builds artifacts, and signs Python distributions.

## Deploy to Dev
1. Merge to `main` triggers the CD workflow.
2. The pipeline builds images, pushes to GHCR, sets image tags in `k8s/aks/`, and applies with Kustomize.
3. Deployment status appears in the GitHub Actions run and environment view.

## Deploy to Staging/Prod
1. Trigger `CD Deploy` via workflow dispatch.
2. Provide environment (`staging` or `prod`) and image tags.
3. Approvals are enforced via GitHub Environments.

## Rollback
1. Identify the deployment:
   - `kubectl get deploy -n <namespace>`
2. Roll back:
   - `kubectl rollout undo deployment/airflow-webserver -n <namespace>`
   - `kubectl rollout undo deployment/airflow-scheduler -n <namespace>`
   - `kubectl rollout undo deployment/frontend -n <namespace>`
