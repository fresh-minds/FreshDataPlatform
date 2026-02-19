# CI/CD Decisions

## Pipeline Platform
- GitHub Actions is already in use, so CI/CD is implemented and extended with GitHub Actions workflows.

## CI Structure
- `ci.yml` handles preflight, lint/format, typecheck, unit tests, integration smoke, and build artifacts.
- `security.yml` handles SAST, secret scanning, dependency scans, container/file system scanning, and SBOM.
- Existing E2E workflows are retained and aligned with caching and least-privilege permissions.

## Security Tooling
- Secret scanning: Gitleaks.
- SAST: CodeQL (Python + JavaScript).
- Dependency scan: `pip-audit` and `npm audit`.
- Container/filesystem scan: Trivy.
- SBOM: Syft via Anchore SBOM action.

## Release Management
- Conventional Commits + Release Please for automated versioning and changelog.
- Release workflow builds artifacts, attaches to GitHub Release, and signs Python distributions using Sigstore.

## Container Registry
- Default to GitHub Container Registry (GHCR) for CI image publishing.

## Deployment
- Kubernetes deployments use Kustomize in `k8s/aks/`.
- Environment gates are enforced via GitHub environments (`dev`, `staging`, `prod`) and required reviewers.
