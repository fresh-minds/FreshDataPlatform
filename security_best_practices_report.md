# Security Best Practices Report (Post-Remediation)

Date: 2026-02-24  
Scope: Python services, scripts, connector parsing paths, env/deployment defaults, and related documentation.

## Summary

The previously identified high-priority issues were remediated in this change:

1. Superset OAuth auto-registration no longer defaults to `Admin`.
2. Superset CSRF is enabled by default.
3. MinIO SSO bridge and kind SSO gateway now fail fast for missing/placeholder secrets.
4. External XML parsing in connectors now uses `defusedxml`.
5. Hardcoded MinIO helper credentials were removed.
6. Portal API JWT verification moved from `python-jose` to `PyJWT[crypto]` and service dependency pins were updated to clear actionable `pip-audit` findings.

## Verification Results

- `pip-audit -r ops/portal-api/requirements.txt`: no known vulnerabilities
- `pip-audit -r ops/minio-sso-bridge/requirements.txt`: no known vulnerabilities
- `npm audit --omit=dev --audit-level=high --package-lock-only` (frontend): no known vulnerabilities
- Targeted `bandit` run across changed security-sensitive files: 0 findings
- `python3 -m py_compile` checks for changed Python modules and `scripts/**/*.py`: pass
- `bash -n` checks for changed shell scripts: pass
- `uvicorn app:app --help` sanity checks for:
  - `ops/minio-sso-bridge/app.py`
  - `ops/portal-api/app.py`

## Residual Risks / Follow-up

- Some legacy local/dev defaults remain intentionally permissive for non-production use (for example admin-style local credentials in `.env.template`); production deployments should continue using managed secret sources and least-privilege role mapping.
