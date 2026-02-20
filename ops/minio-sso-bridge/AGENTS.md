# MinIO SSO Bridge Agent Instructions

Scope: `ops/minio-sso-bridge/` (FastAPI bridge for Keycloak -> MinIO console SSO flow).

## Must Do
- Keep security-sensitive behavior explicit:
  - state/nonce validation
  - token exchange paths
  - cookie and redirect handling
- When environment variables are added/changed, update:
  - `.env.template`
  - related deployment manifests/scripts that pass those vars

## Validation
- Run and report:
  - `python3 -m py_compile ops/minio-sso-bridge/app.py`
  - basic import/start sanity (for example `uvicorn app:app --help` or equivalent)

## Documentation Requirements
- Update:
  - `k8s/README.md` and `DEPLOYMENT.md` for runtime/deployment behavior changes
  - `README.md` / `DEVELOPMENT.md` when local run/setup expectations change

## Change Rules
- Keep `requirements.txt` synchronized with runtime imports in `app.py`.
- Avoid adding new auth behavior without corresponding test coverage in `tests/sso/` when applicable.
