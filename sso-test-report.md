# SSO E2E Test Report

- Generated at: 2026-02-17T19:58:08.798930+00:00
- Environment: `dev`
- Overall result: **FAIL**
- Inventory source: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/sso-test-inventory.md`

## Summary

- Total tests: 22
- Passed: 10
- Failed: 7
- Skipped: 5

## Apps x Flows

| App | Smoke | Browser Login | Cross-App SSO | Logout | Session Expiry | API AuthZ | Negative Security |
|---|---|---|---|---|---|---|---|
| airflow | FAIL | FAIL | FAIL | FAIL | SKIP | FAIL | PASS |
| datahub | FAIL | FAIL | FAIL | FAIL | SKIP | FAIL | PASS |
| minio | FAIL | FAIL | FAIL | FAIL | SKIP | FAIL | PASS |

## Evidence

- JUnit XML: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/junit-20260217T195805Z.xml`
- Artifacts root: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260217T195805Z`
- HAR files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260217T195805Z/har`
- Trace files: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260217T195805Z/traces`
- Screenshots: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260217T195805Z/screenshots`
- Logs: `/Users/karelgoense/Documents/programming/FreshMinds_Programming/production/ai_trial/tests/sso/artifacts/20260217T195805Z/logs`

## Failures and Fixes

- Test: `tests.sso.test_api_authorization::test_access_token_claim_mapping`
- Evidence: AssertionError: aud claim missing assert (False)  +  where False = isinstance(None, str)
- Likely root cause: See assertion details and request/response artifacts.
- Remediation: Apply targeted fix based on failing assertion, then rerun sso-e2e.
- Test: `tests.sso.test_browser_flows::test_unauthenticated_access_redirects_to_keycloak[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Check app auth middleware and OIDC client settings; ensure unauthenticated routes force IdP redirect instead of local fallback login paths.
- Remediation: Check app auth middleware and OIDC client settings; ensure unauthenticated routes force IdP redirect instead of local fallback login paths.
- Test: `tests.sso.test_browser_flows::test_login_establishes_session_for_each_app[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: See assertion details and request/response artifacts.
- Remediation: Apply targeted fix based on failing assertion, then rerun sso-e2e.
- Test: `tests.sso.test_browser_flows::test_cross_app_sso_uses_existing_session[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Remediation: Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login or isolate auth context by origin policy.
- Test: `tests.sso.test_browser_flows::test_logout_propagation_matches_expected_design[chromium]`
- Evidence: playwright._impl._errors.Error: Page.goto: net::ERR_CONNECTION_REFUSED at http://localhost:8080/ Call log:   - navigating to "http://localhost:8080/", waiting until "domcontentloaded"
- Likely root cause: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.
- Remediation: Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session to prevent stale sessions across apps.
- Test: `tests.sso.test_smoke_config::test_client_redirect_uris_match_inventory`
- Evidence: AssertionError: Client 'airflow' uses wildcard webOrigins. Replace '*' with explicit origins to reduce token leakage and CORS risk. assert '*' not in {'*'}
- Likely root cause: Update Keycloak client config to align redirect URIs/origins with deployed app URLs; remove wildcard origins and explicitly list trusted origins.
- Remediation: Update Keycloak client config to align redirect URIs/origins with deployed app URLs; remove wildcard origins and explicitly list trusted origins.
- Test: `tests.sso.test_smoke_config::test_auth_code_pkce_token_exchange_per_client`
- Evidence: AssertionError: Missing aud claim in access token for airflow assert 'aud' in {'exp': 1771358588, 'iat': 1771358288, 'auth_time': 1771358288, 'jti': 'c483d8c4-17c7-444d-a2be-9c68e034b1c3', 'iss': 'http://localhost:8090/realms/odp', 'sub': 'd17bf29a-f67c-432f-9be5-f87df4c1c98a', '...
- Likely root cause: Verify client credentials and redirect URIs; ensure app is configured for auth code + PKCE and token endpoint accepts the configured client authentication method.
- Remediation: Verify client credentials and redirect URIs; ensure app is configured for auth code + PKCE and token endpoint accepts the configured client authentication method.

## Repro Steps

1. Set SSO environment variables in `.env` or shell.
2. Start the target stack (local/dev/stage) with Keycloak + integrated apps.
3. Run `make test-sso`.
4. Inspect `tests/sso/artifacts/latest` and this report.
