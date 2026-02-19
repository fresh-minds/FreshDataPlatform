# Deploy Bundle

Production-oriented Kubernetes bundle for Open Data Platform core services.

## Layout

- `base/`: shared hardened manifests
  - includes `secret-odp-env.template.yaml` as a non-applied secret template
- `overlays/dev`: low-cost dev profile
- `overlays/staging`: pre-prod profile
- `overlays/prod`: production profile
- `images/`: optional hardened frontend image assets
- `RUNBOOK.md`: build, deploy, verify, rollback, troubleshooting
