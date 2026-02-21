# Deploy Bundle Agent Instructions

Scope: `deploy/` (production-oriented Kubernetes bundle with Kustomize overlays).

## Must Do
- Keep `base/` reusable and environment-agnostic.
- Keep environment-specific changes in `overlays/*`.
- Do not place real secret values in tracked files; keep templates only.

## Validation
- Build and validate affected overlays:
  - `kubectl kustomize deploy/overlays/dev >/dev/null`
  - `kubectl kustomize deploy/overlays/staging >/dev/null`
  - `kubectl kustomize deploy/overlays/prod >/dev/null`
- If `kubectl kustomize` is unavailable, use `kustomize build`.

## Documentation Requirements
- Update:
  - `deploy/README.md`
  - `deploy/RUNBOOK.md`
  - `DEPLOYMENT.md` when operator-facing rollout steps change
