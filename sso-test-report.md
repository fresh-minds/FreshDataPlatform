# SSO E2E Test Report

- Generated at: 2026-02-25T05:47:42.846292+00:00
- Environment: `dev`
- Overall result: **FAIL**
- Inventory source: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/sso-test-inventory.md`

## Summary

- Total tests: 24
- Passed: 15
- Failed: 4
- Skipped: 5

## Apps x Flows

| App | Smoke | Browser Login | Cross-App SSO | Logout | Session Expiry | API AuthZ | Negative Security |
|---|---|---|---|---|---|---|---|
| airflow | PASS | FAIL | FAIL | FAIL | SKIP | PASS | PASS |
| datahub | PASS | FAIL | FAIL | FAIL | SKIP | PASS | PASS |
| minio | PASS | FAIL | FAIL | FAIL | SKIP | PASS | PASS |
| superset | PASS | FAIL | FAIL | FAIL | SKIP | PASS | PASS |

## Evidence

- JUnit XML: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/junit-20260225T054613Z.xml`
- Artifacts root: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260225T054614Z`
- HAR files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260225T054614Z/har`
- Trace files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260225T054614Z/traces`
- Screenshots: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260225T054614Z/screenshots`
- Logs: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260225T054614Z/logs`

## Failures and Fixes

- Test: `tests.sso.test_browser_flows::test_login_establishes_session_for_each_app[chromium]`
- Evidence: playwright._impl._errors.TimeoutError: Locator.wait_for: Timeout 20000ms exceeded. Call log:   - waiting for locator("#username, input[name='username']").first to be visible     45 × locator resolved to hidden <input type="text" name="username" value="odp-admin" autocomplete="use...
- Likely root cause: See assertion details and request/response artifacts.
- Remediation: Apply targeted fix based on failing assertion, then rerun sso-e2e.
- Test: `tests.sso.test_browser_flows::test_minio_sso_bridge_login_flow[chromium]`
- Evidence: playwright._impl._errors.TimeoutError: Locator.wait_for: Timeout 20000ms exceeded. Call log:   - waiting for locator("#username, input[name='username']").first to be visible     45 × locator resolved to hidden <input type="text" name="username" value="odp-admin" autocomplete="use...
- Likely root cause: Ensure the bridge uses browser-reachable Keycloak authorize URL, callback URI is allowlisted on client 'minio', and bridge forwards MinIO console session cookies after STS login.
- Remediation: Ensure the bridge uses browser-reachable Keycloak authorize URL, callback URI is allowlisted on client 'minio', and bridge forwards MinIO console session cookies after STS login.
- Test: `tests.sso.test_browser_flows::test_cross_app_sso_uses_existing_session[chromium]`
- Evidence: playwright._impl._errors.TimeoutError: Locator.wait_for: Timeout 20000ms exceeded. Call log:   - waiting for locator("#username, input[name='username']").first to be visible     45 × locator resolved to hidden <input type="text" name="username" value="odp-admin" autocomplete="use...
- Likely root cause: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Remediation: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Test: `tests.sso.test_browser_flows::test_logout_propagation_matches_expected_design[chromium]`
- Evidence: playwright._impl._errors.TimeoutError: Locator.wait_for: Timeout 20000ms exceeded. Call log:   - waiting for locator("#username, input[name='username']").first to be visible     45 × locator resolved to hidden <input type="text" name="username" value="odp-admin" autocomplete="use...
- Likely root cause: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.
- Remediation: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.

## Repro Steps

1. Set SSO environment variables in `.env` or shell.
2. Start the target stack (local/dev/stage) with Keycloak + integrated apps.
3. Run `make test-sso`.
4. Inspect `tests/sso/artifacts/latest` and this report.
