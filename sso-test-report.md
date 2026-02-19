# SSO E2E Test Report

- Generated at: 2026-02-19T04:39:55.290719+00:00
- Environment: `dev`
- Overall result: **FAIL**
- Inventory source: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/sso-test-inventory.md`

## Summary

- Total tests: 24
- Passed: 7
- Failed: 10
- Skipped: 7

## Apps x Flows

| App | Smoke | Browser Login | Cross-App SSO | Logout | Session Expiry | API AuthZ | Negative Security |
|---|---|---|---|---|---|---|---|
| airflow | FAIL | FAIL | FAIL | FAIL | SKIP | SKIP | FAIL |
| datahub | FAIL | FAIL | FAIL | FAIL | SKIP | SKIP | FAIL |
| minio | FAIL | FAIL | FAIL | FAIL | SKIP | SKIP | FAIL |
| superset | FAIL | FAIL | FAIL | FAIL | SKIP | SKIP | FAIL |

## Evidence

- JUnit XML: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/junit-20260219T043916Z.xml`
- Artifacts root: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260219T043922Z`
- HAR files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260219T043922Z/har`
- Trace files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260219T043922Z/traces`
- Screenshots: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260219T043922Z/screenshots`
- Logs: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260219T043922Z/logs`

## Failures and Fixes

- Test: `tests.sso.test_browser_flows::test_unauthenticated_access_redirects_to_keycloak[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Check app auth middleware and OIDC client settings; ensure unauthenticated routes force IdP redirect instead of local fallback login paths.
- Remediation: Check app auth middleware and OIDC client settings; ensure unauthenticated routes force IdP redirect instead of local fallback login paths.
- Test: `tests.sso.test_browser_flows::test_login_establishes_session_for_each_app[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: See assertion details and request/response artifacts.
- Remediation: Apply targeted fix based on failing assertion, then rerun sso-e2e.
- Test: `tests.sso.test_browser_flows::test_minio_sso_bridge_login_flow[chromium]`
- Evidence: AssertionError: MinIO SSO bridge did not land on http://localhost:9001/browser after login; got http://localhost:8090/realms/odp/login-actions/authenticate?execution=240ec6a2-e47e-466e-b3a8-f836fc7523fb&client_id=minio&tab_id=eOslxf7OtkA&client_data=eyJydSI6Imh0dHA6Ly9sb2NhbGhvc3...
- Likely root cause: Ensure the bridge uses browser-reachable Keycloak authorize URL, callback URI is allowlisted on client 'minio', and bridge forwards MinIO console session cookies after STS login.
- Remediation: Ensure the bridge uses browser-reachable Keycloak authorize URL, callback URI is allowlisted on client 'minio', and bridge forwards MinIO console session cookies after STS login.
- Test: `tests.sso.test_browser_flows::test_cross_app_sso_uses_existing_session[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Remediation: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Test: `tests.sso.test_browser_flows::test_logout_propagation_matches_expected_design[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.
- Remediation: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.
- Test: `tests.sso.test_negative_security::test_invalid_code_verifier_rejected`
- Evidence: scripts.sso.oidc.OIDCFlowError: Login did not complete; Keycloak login form remained visible
- Likely root cause: Enforce PKCE verifier checks on token exchange and disable non-PKCE fallbacks for public clients.
- Remediation: Enforce PKCE verifier checks on token exchange and disable non-PKCE fallbacks for public clients.
- Test: `tests.sso.test_negative_security::test_token_substitution_audience_protection`
- Evidence: scripts.sso.oidc.OIDCFlowError: Login did not complete; Keycloak login form remained visible
- Likely root cause: Validate audience and azp checks on every API/gateway so tokens from other clients are rejected.
- Remediation: Validate audience and azp checks on every API/gateway so tokens from other clients are rejected.
- Test: `tests.sso.test_negative_security::test_clock_skew_handling_sanity`
- Evidence: scripts.sso.oidc.OIDCFlowError: Login did not complete; Keycloak login form remained visible
- Likely root cause: See assertion details and request/response artifacts.
- Remediation: Apply targeted fix based on failing assertion, then rerun sso-e2e.
- Test: `tests.sso.test_negative_security::test_refresh_token_rotation_behavior`
- Evidence: scripts.sso.oidc.OIDCFlowError: Login did not complete; Keycloak login form remained visible
- Likely root cause: Enable refresh token rotation + reuse detection in realm/client settings if required by policy.
- Remediation: Enable refresh token rotation + reuse detection in realm/client settings if required by policy.
- Test: `tests.sso.test_smoke_config::test_auth_code_pkce_token_exchange_per_client`
- Evidence: scripts.sso.oidc.OIDCFlowError: Login did not complete; Keycloak login form remained visible
- Likely root cause: Verify client credentials and redirect URIs; ensure app is configured for auth code + PKCE and token endpoint accepts the configured client authentication method.
- Remediation: Verify client credentials and redirect URIs; ensure app is configured for auth code + PKCE and token endpoint accepts the configured client authentication method.

## Repro Steps

1. Set SSO environment variables in `.env` or shell.
2. Start the target stack (local/dev/stage) with Keycloak + integrated apps.
3. Run `make test-sso`.
4. Inspect `tests/sso/artifacts/latest` and this report.
