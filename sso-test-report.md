# SSO E2E Test Report

- Generated at: 2026-02-18T15:38:33.521745+00:00
- Environment: `dev`
- Overall result: **PASS**
- Inventory source: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/sso-test-inventory.md`

## Summary

- Total tests: 24
- Passed: 19
- Failed: 0
- Skipped: 5

## Apps x Flows

| App | Smoke | Browser Login | Cross-App SSO | Logout | Session Expiry | API AuthZ | Negative Security |
|---|---|---|---|---|---|---|---|
| airflow | PASS | PASS | PASS | PASS | SKIP | PASS | PASS |
| datahub | PASS | PASS | PASS | PASS | SKIP | PASS | PASS |
| minio | PASS | PASS | PASS | PASS | SKIP | PASS | PASS |
| superset | PASS | PASS | PASS | PASS | SKIP | PASS | PASS |

## Evidence

- JUnit XML: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/junit-20260218T153808Z.xml`
- Artifacts root: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260218T153809Z`
- HAR files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260218T153809Z/har`
- Trace files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260218T153809Z/traces`
- Screenshots: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260218T153809Z/screenshots`
- Logs: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260218T153809Z/logs`

## Failures and Fixes

- No failing tests were observed in this run.

## Repro Steps

1. Set SSO environment variables in `.env` or shell.
2. Start the target stack (local/dev/stage) with Keycloak + integrated apps.
3. Run `make test-sso`.
4. Inspect `tests/sso/artifacts/latest` and this report.
