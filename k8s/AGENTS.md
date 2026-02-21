# Kubernetes Agent Instructions

Scope: `k8s/` (local kind and AKS manifests under `dev/` and `aks/`).

## Must Do
- Treat `k8s/dev/` and `k8s/aks/` as related deployment paths; if a service contract changes in one, evaluate impact in the other.
- Keep manifests namespaced and consistent with scripts in `scripts/k8s_*.sh` and `scripts/aks_*.sh`.
- Never commit real secrets into manifests.

## Validation
- Validate changed overlays/manifests with:
  - `kubectl kustomize k8s/dev >/dev/null`
  - `kubectl kustomize k8s/aks >/dev/null`
  - If `kubectl kustomize` is unavailable, use `kustomize build`.

## Documentation Requirements
- Update:
  - `k8s/README.md`
  - `DEPLOYMENT.md`
  - any affected bootstrap scripts/docs when ports, hosts, or ingress behavior changes
