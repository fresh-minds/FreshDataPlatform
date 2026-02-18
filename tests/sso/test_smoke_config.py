from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.sso.oidc import (
    decode_jwt_claims,
    extract_authorization_query,
    fetch_jwks,
    fetch_openid_configuration,
    fetch_tokens_auth_code_pkce,
)
from tests.sso.settings import SSOSettings


@pytest.mark.e2e
def test_openid_configuration_reachable_for_each_realm(
    sso_settings: SSOSettings,
    api_client: object,
    write_log: object,
) -> None:
    for realm in sso_settings.keycloak_realms:
        url = f"{sso_settings.keycloak_base_url}/realms/{realm}/.well-known/openid-configuration"
        response = api_client.get(url)
        write_log(f"discovery-{realm}", response.text)

        assert response.status_code == 200, f"Discovery endpoint failed for realm={realm}: {response.status_code}"

        payload = response.json()
        for required_field in ("issuer", "authorization_endpoint", "token_endpoint", "jwks_uri"):
            assert required_field in payload, f"Missing {required_field} in discovery payload for realm={realm}"


@pytest.mark.e2e
def test_jwks_endpoint_reachable_and_keys_parseable(sso_settings: SSOSettings) -> None:
    metadata = fetch_openid_configuration(
        sso_settings.keycloak_base_url,
        sso_settings.keycloak_primary_realm,
    )
    jwks = fetch_jwks(metadata.jwks_uri)
    keys = jwks.get("keys", [])

    assert isinstance(keys, list) and keys, "JWKS does not contain keys"
    for index, key in enumerate(keys):
        assert isinstance(key, dict), f"JWKS key[{index}] is not an object"
        assert key.get("kid"), f"JWKS key[{index}] is missing kid"
        assert key.get("kty"), f"JWKS key[{index}] is missing kty"


@pytest.mark.e2e
def test_client_redirect_uris_match_inventory(sso_settings: SSOSettings) -> None:
    realm_import_path: Path = sso_settings.realm_import_path
    if not realm_import_path.exists():
        pytest.skip(f"Realm import file not found: {realm_import_path}")

    realm_payload = json.loads(realm_import_path.read_text(encoding="utf-8"))
    clients = {client["clientId"]: client for client in realm_payload.get("clients", [])}

    for app in sso_settings.apps:
        client = clients.get(app.client_id)
        assert client is not None, f"Client '{app.client_id}' missing from realm import file"

        redirect_uris = set(client.get("redirectUris", []))
        assert app.redirect_uri in redirect_uris, (
            f"{app.name} redirect URI '{app.redirect_uri}' missing for client '{app.client_id}'. "
            f"Found={sorted(redirect_uris)}"
        )

        if app.name in {"airflow", "datahub", "minio"}:
            web_origins = set(client.get("webOrigins", []))
            assert "*" not in web_origins, (
                f"Client '{app.client_id}' uses wildcard webOrigins. "
                "Replace '*' with explicit origins to reduce token leakage and CORS risk."
            )


@pytest.mark.e2e
def test_auth_code_pkce_token_exchange_per_client(sso_settings: SSOSettings, write_log: object) -> None:
    if not sso_settings.basic_user.username or not sso_settings.basic_user.password:
        pytest.skip("Basic user credentials are missing")

    for app in sso_settings.apps:
        tokens = fetch_tokens_auth_code_pkce(
            keycloak_base_url=sso_settings.keycloak_base_url,
            realm=sso_settings.keycloak_primary_realm,
            client_id=app.client_id,
            client_secret=app.client_secret,
            redirect_uri=app.redirect_uri,
            username=sso_settings.basic_user.username,
            password=sso_settings.basic_user.password,
        )

        auth_query = extract_authorization_query(str(tokens["_meta"]["auth_url"]))
        if sso_settings.require_pkce:
            assert auth_query.get("code_challenge"), f"PKCE code_challenge missing for {app.name}"
            assert auth_query.get("code_challenge_method") == "S256", f"PKCE method is not S256 for {app.name}"

        assert auth_query.get("response_type") == "code", f"{app.name} is not using authorization code flow"
        assert auth_query.get("client_id") == app.client_id, f"Unexpected client_id in auth flow for {app.name}"

        access_token = tokens.get("access_token")
        assert access_token, f"Token endpoint did not return access_token for {app.name}"

        claims = decode_jwt_claims(access_token)
        for required_claim in ("iss", "aud", "azp", "exp", "iat", "sub"):
            assert required_claim in claims, f"Missing {required_claim} claim in access token for {app.name}"

        write_log(
            f"token-claims-{app.name}",
            json.dumps({"app": app.name, "claims": claims}, indent=2),
        )


@pytest.mark.e2e
def test_https_enforcement_when_required(sso_settings: SSOSettings) -> None:
    if not sso_settings.require_https:
        pytest.skip("HTTPS enforcement check disabled (SSO_REQUIRE_HTTPS=false)")

    assert sso_settings.keycloak_base_url.startswith("https://"), "Keycloak base URL must use HTTPS"
    for app in sso_settings.apps:
        assert app.base_url.startswith("https://"), f"{app.name} URL must use HTTPS"


@pytest.mark.e2e
def test_logout_endpoint_published(keycloak_metadata: dict[str, object]) -> None:
    assert keycloak_metadata.get("end_session_endpoint"), "OIDC end_session_endpoint is missing"
