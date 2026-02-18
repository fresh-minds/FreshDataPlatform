from __future__ import annotations

import json
from typing import Any

import pytest

from scripts.sso.oidc import (
    decode_jwt_claims,
    fetch_tokens_auth_code_pkce,
    password_grant_token,
)
from tests.sso.settings import SSOApp, SSOSettings


def _bearer(token: str) -> dict[str, str]:
    return {"Authorization": f"Bearer {token}"}


@pytest.fixture(scope="session")
def first_app(sso_settings: SSOSettings) -> SSOApp:
    if not sso_settings.apps:
        pytest.skip("No SSO apps configured")
    return sso_settings.apps[0]


@pytest.fixture(scope="session")
def issued_tokens(sso_settings: SSOSettings, first_app: SSOApp) -> dict[str, dict[str, Any]]:
    tokens: dict[str, dict[str, Any]] = {}

    try:
        basic = fetch_tokens_auth_code_pkce(
            keycloak_base_url=sso_settings.keycloak_base_url,
            realm=sso_settings.keycloak_primary_realm,
            client_id=first_app.client_id,
            client_secret=first_app.client_secret,
            redirect_uri=first_app.redirect_uri,
            username=sso_settings.basic_user.username,
            password=sso_settings.basic_user.password,
        )
        tokens["basic"] = basic
    except Exception as exc:  # noqa: BLE001
        pytest.skip(f"Unable to fetch basic user token: {exc}")

    if sso_settings.no_access_user:
        try:
            tokens["no_access"] = fetch_tokens_auth_code_pkce(
                keycloak_base_url=sso_settings.keycloak_base_url,
                realm=sso_settings.keycloak_primary_realm,
                client_id=first_app.client_id,
                client_secret=first_app.client_secret,
                redirect_uri=first_app.redirect_uri,
                username=sso_settings.no_access_user.username,
                password=sso_settings.no_access_user.password,
            )
        except Exception:
            pass

    if sso_settings.admin_user:
        try:
            tokens["realm_admin"] = fetch_tokens_auth_code_pkce(
                keycloak_base_url=sso_settings.keycloak_base_url,
                realm=sso_settings.keycloak_primary_realm,
                client_id=first_app.client_id,
                client_secret=first_app.client_secret,
                redirect_uri=first_app.redirect_uri,
                username=sso_settings.admin_user.username,
                password=sso_settings.admin_user.password,
            )
        except Exception:
            pass

    if sso_settings.keycloak_admin_user and sso_settings.keycloak_admin_password:
        try:
            tokens["kc_admin"] = password_grant_token(
                keycloak_base_url=sso_settings.keycloak_base_url,
                realm="master",
                client_id="admin-cli",
                username=sso_settings.keycloak_admin_user,
                password=sso_settings.keycloak_admin_password,
            )
        except Exception:
            pass

    return tokens


@pytest.mark.e2e
def test_access_token_claim_mapping(
    sso_settings: SSOSettings,
    keycloak_metadata: dict[str, object],
    issued_tokens: dict[str, dict[str, Any]],
    first_app: SSOApp,
    write_log: object,
) -> None:
    access_token = issued_tokens["basic"]["access_token"]
    claims = decode_jwt_claims(access_token)

    expected_issuer = str(keycloak_metadata["issuer"])
    assert claims.get("iss") == expected_issuer, "Unexpected issuer claim"

    aud_claim = claims.get("aud")
    if isinstance(aud_claim, list):
        assert aud_claim, "aud claim list is empty"
    else:
        assert isinstance(aud_claim, str) and aud_claim, "aud claim missing"

    assert claims.get("azp") == first_app.client_id, "azp claim does not match client"
    assert isinstance(claims.get("exp"), int), "exp claim missing/invalid"
    assert isinstance(claims.get("iat"), int), "iat claim missing/invalid"
    assert claims.get("sub"), "sub claim missing"

    realm_roles = claims.get("realm_access", {}).get("roles", [])
    for role in sso_settings.expected_realm_roles:
        assert role in realm_roles, f"Missing expected realm role '{role}'"

    groups = claims.get("groups", [])
    for group in sso_settings.expected_groups:
        assert group in groups, f"Missing expected group '{group}'"

    for client_id, role in sso_settings.expected_resource_roles:
        roles = claims.get("resource_access", {}).get(client_id, {}).get("roles", [])
        assert role in roles, f"Missing resource role '{role}' for client '{client_id}'"

    write_log("api-access-token-claims", json.dumps(claims, indent=2))


@pytest.mark.e2e
def test_keycloak_userinfo_authorization(
    keycloak_metadata: dict[str, object],
    issued_tokens: dict[str, dict[str, Any]],
    api_client: object,
) -> None:
    userinfo_endpoint = keycloak_metadata.get("userinfo_endpoint")
    if not userinfo_endpoint:
        pytest.skip("userinfo endpoint not published by discovery")

    no_token = api_client.get(str(userinfo_endpoint))
    assert no_token.status_code == 401, f"Expected 401 without token, got {no_token.status_code}"

    invalid = api_client.get(str(userinfo_endpoint), headers=_bearer("not-a-jwt"))
    assert invalid.status_code == 401, f"Expected 401 for invalid token, got {invalid.status_code}"

    valid = api_client.get(
        str(userinfo_endpoint),
        headers=_bearer(issued_tokens["basic"]["access_token"]),
    )
    assert valid.status_code == 200, f"Expected 200 with valid token, got {valid.status_code}"


@pytest.mark.e2e
def test_keycloak_admin_api_requires_elevated_role(
    sso_settings: SSOSettings,
    issued_tokens: dict[str, dict[str, Any]],
    api_client: object,
) -> None:
    admin_endpoint = (
        f"{sso_settings.keycloak_base_url}/admin/realms/{sso_settings.keycloak_primary_realm}/clients"
    )

    no_token = api_client.get(admin_endpoint)
    assert no_token.status_code in {401, 403}, f"Expected unauthorized without token, got {no_token.status_code}"

    basic = api_client.get(
        admin_endpoint,
        headers=_bearer(issued_tokens["basic"]["access_token"]),
    )
    assert basic.status_code in {401, 403}, (
        f"Expected 401/403 for non-admin token on admin API, got {basic.status_code}"
    )

    kc_admin = issued_tokens.get("kc_admin")
    if not kc_admin:
        pytest.skip("Keycloak admin credentials unavailable; skipping admin success assertion")

    elevated = api_client.get(admin_endpoint, headers=_bearer(kc_admin["access_token"]))
    assert elevated.status_code == 200, f"Expected 200 for admin token, got {elevated.status_code}"


@pytest.mark.e2e
def test_configured_protected_api_matrix(
    sso_settings: SSOSettings,
    issued_tokens: dict[str, dict[str, Any]],
    api_client: object,
) -> None:
    if not sso_settings.protected_apis:
        pytest.skip("No protected API matrix configured (SSO_API_MATRIX_JSON)")

    basic_token = issued_tokens["basic"]["access_token"]
    no_access_token = issued_tokens.get("no_access", {}).get("access_token")

    for api in sso_settings.protected_apis:
        no_token = api_client.get(api.url)
        assert no_token.status_code in {401, 403}, (
            f"{api.name}: expected 401/403 without token, got {no_token.status_code}"
        )

        invalid = api_client.get(api.url, headers=_bearer("invalid-token"))
        assert invalid.status_code in {401, 403}, (
            f"{api.name}: expected 401/403 for invalid token, got {invalid.status_code}"
        )

        success = api_client.get(api.url, headers=_bearer(basic_token))
        assert success.status_code == api.success_status, (
            f"{api.name}: expected {api.success_status} with valid token, got {success.status_code}"
        )

        if api.missing_role_status is not None and no_access_token:
            missing_role = api_client.get(api.url, headers=_bearer(no_access_token))
            assert missing_role.status_code == api.missing_role_status, (
                f"{api.name}: expected {api.missing_role_status} for missing-role token, got "
                f"{missing_role.status_code}"
            )
