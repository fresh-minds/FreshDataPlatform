from __future__ import annotations

import os

import pytest

from scripts.sso.oidc import (
    OIDCFlowError,
    decode_jwt_claims,
    exchange_code_for_tokens,
    fetch_openid_configuration,
    fetch_tokens_auth_code_pkce,
    refresh_access_token,
    request_authorization_code,
    token_is_expired,
    token_not_yet_valid,
)
from tests.sso.settings import SSOApp, SSOSettings


@pytest.fixture(scope="session")
def primary_app(sso_settings: SSOSettings) -> SSOApp:
    if not sso_settings.apps:
        pytest.skip("No SSO apps configured")
    return sso_settings.apps[0]


@pytest.mark.e2e
def test_redirect_uri_injection_rejected(
    sso_settings: SSOSettings,
    api_client: object,
    primary_app: SSOApp,
) -> None:
    metadata = fetch_openid_configuration(
        sso_settings.keycloak_base_url,
        sso_settings.keycloak_primary_realm,
    )

    malicious_redirect = "https://attacker.invalid/callback"
    response = api_client.get(
        metadata.authorization_endpoint,
        params={
            "client_id": primary_app.client_id,
            "response_type": "code",
            "scope": "openid profile email",
            "redirect_uri": malicious_redirect,
            "state": "state123",
            "nonce": "nonce123",
            "code_challenge": "abc",
            "code_challenge_method": "S256",
        },
    )

    assert response.status_code in {400, 401}, f"Expected redirect URI rejection, got {response.status_code}"
    assert "redirect" in response.text.lower(), "Expected redirect URI error details"


@pytest.mark.e2e
def test_invalid_code_verifier_rejected(
    sso_settings: SSOSettings,
    primary_app: SSOApp,
) -> None:
    metadata = fetch_openid_configuration(
        sso_settings.keycloak_base_url,
        sso_settings.keycloak_primary_realm,
    )

    auth = request_authorization_code(
        metadata=metadata,
        client_id=primary_app.client_id,
        redirect_uri=primary_app.redirect_uri,
        username=sso_settings.basic_user.username,
        password=sso_settings.basic_user.password,
    )

    with pytest.raises(OIDCFlowError):
        exchange_code_for_tokens(
            metadata=metadata,
            client_id=primary_app.client_id,
            client_secret=primary_app.client_secret,
            redirect_uri=primary_app.redirect_uri,
            code=auth["code"],
            code_verifier="tampered-verifier",
        )


@pytest.mark.e2e
def test_token_substitution_audience_protection(sso_settings: SSOSettings) -> None:
    if len(sso_settings.apps) < 2:
        pytest.skip("Need at least two apps for token substitution test")

    source_app = sso_settings.apps[0]
    target_app = sso_settings.apps[1]

    tokens = fetch_tokens_auth_code_pkce(
        keycloak_base_url=sso_settings.keycloak_base_url,
        realm=sso_settings.keycloak_primary_realm,
        client_id=source_app.client_id,
        client_secret=source_app.client_secret,
        redirect_uri=source_app.redirect_uri,
        username=sso_settings.basic_user.username,
        password=sso_settings.basic_user.password,
    )

    claims = decode_jwt_claims(tokens["access_token"])
    aud_claim = claims.get("aud")

    if isinstance(aud_claim, str):
        aud_values = {aud_claim}
    elif isinstance(aud_claim, list):
        aud_values = set(str(item) for item in aud_claim)
    else:
        aud_values = set()

    assert target_app.client_id not in aud_values, (
        f"Token minted for {source_app.client_id} unexpectedly includes audience {target_app.client_id}"
    )

    substitution_url = os.getenv("SSO_TOKEN_SUBSTITUTION_URL")
    if substitution_url:
        import httpx

        response = httpx.get(
            substitution_url,
            headers={"Authorization": f"Bearer {tokens['access_token']}"},
            timeout=20.0,
        )
        assert response.status_code in {401, 403}, (
            f"Substitution endpoint accepted wrong-audience token ({response.status_code})"
        )


@pytest.mark.e2e
def test_clock_skew_handling_sanity(sso_settings: SSOSettings, primary_app: SSOApp) -> None:
    tokens = fetch_tokens_auth_code_pkce(
        keycloak_base_url=sso_settings.keycloak_base_url,
        realm=sso_settings.keycloak_primary_realm,
        client_id=primary_app.client_id,
        client_secret=primary_app.client_secret,
        redirect_uri=primary_app.redirect_uri,
        username=sso_settings.basic_user.username,
        password=sso_settings.basic_user.password,
    )

    claims = decode_jwt_claims(tokens["access_token"])
    iat = int(claims.get("iat", 0))
    exp = int(claims.get("exp", 0))

    assert not token_not_yet_valid(claims, now=iat + 30, leeway_seconds=60), (
        "Token should remain valid under small positive clock skew"
    )
    assert token_is_expired(claims, now=exp + 3600, leeway_seconds=60), (
        "Token should be treated as expired under large clock skew"
    )


@pytest.mark.e2e
def test_refresh_token_rotation_behavior(sso_settings: SSOSettings, primary_app: SSOApp) -> None:
    tokens = fetch_tokens_auth_code_pkce(
        keycloak_base_url=sso_settings.keycloak_base_url,
        realm=sso_settings.keycloak_primary_realm,
        client_id=primary_app.client_id,
        client_secret=primary_app.client_secret,
        redirect_uri=primary_app.redirect_uri,
        username=sso_settings.basic_user.username,
        password=sso_settings.basic_user.password,
    )

    refresh_token = tokens.get("refresh_token")
    if not refresh_token:
        pytest.skip("No refresh token returned; cannot test rotation")

    first_refresh = refresh_access_token(
        metadata=fetch_openid_configuration(
            sso_settings.keycloak_base_url,
            sso_settings.keycloak_primary_realm,
        ),
        client_id=primary_app.client_id,
        client_secret=primary_app.client_secret,
        refresh_token=refresh_token,
    )
    second_refresh = refresh_access_token(
        metadata=fetch_openid_configuration(
            sso_settings.keycloak_base_url,
            sso_settings.keycloak_primary_realm,
        ),
        client_id=primary_app.client_id,
        client_secret=primary_app.client_secret,
        refresh_token=refresh_token,
    )

    expected_rotation = sso_settings.expected_refresh_rotation
    if expected_rotation is None:
        pytest.skip(
            "Rotation expectation not configured (set SSO_EXPECT_REFRESH_ROTATION=true|false to enforce)"
        )

    if expected_rotation:
        assert first_refresh.status_code == 200, "First refresh should succeed"
        assert second_refresh.status_code in {400, 401}, (
            "Refresh token reuse should fail when rotation is enabled"
        )
    else:
        assert first_refresh.status_code == 200, "First refresh should succeed"
        assert second_refresh.status_code == 200, (
            "Second refresh should succeed when rotation is disabled"
        )


@pytest.mark.e2e
def test_logout_capabilities_discovery(sso_settings: SSOSettings, api_client: object) -> None:
    discovery_url = (
        f"{sso_settings.keycloak_base_url}/realms/{sso_settings.keycloak_primary_realm}"
        "/.well-known/openid-configuration"
    )
    payload = api_client.get(discovery_url).json()

    require_backchannel = os.getenv("SSO_REQUIRE_BACKCHANNEL_LOGOUT", "false").lower() in {"1", "true", "yes", "on"}
    require_frontchannel = os.getenv("SSO_REQUIRE_FRONTCHANNEL_LOGOUT", "false").lower() in {"1", "true", "yes", "on"}

    if require_backchannel:
        assert payload.get("backchannel_logout_supported") is True, (
            "Expected backchannel logout support but discovery does not advertise it"
        )

    if require_frontchannel:
        assert payload.get("frontchannel_logout_supported") is True, (
            "Expected frontchannel logout support but discovery does not advertise it"
        )
