from __future__ import annotations

from dataclasses import dataclass

import pytest

import scripts.verify_keycloak_resources as verify


def test_resolve_keycloak_base_and_realm_prefers_browser_reachable_host(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEYCLOAK_BASE_URL", raising=False)
    monkeypatch.delenv("KEYCLOAK_REALM", raising=False)
    monkeypatch.setenv(
        "KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL",
        "http://keycloak:8090/realms/odp/protocol/openid-connect/auth",
    )
    monkeypatch.setattr(verify.socket, "gethostbyname", lambda _host: (_ for _ in ()).throw(OSError("no dns")))

    base, realm = verify.resolve_keycloak_base_and_realm()

    assert base == "http://localhost:8090"
    assert realm == "odp"


def test_build_client_configs_requires_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("KEYCLOAK_AIRFLOW_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("KEYCLOAK_DATAHUB_CLIENT_SECRET", raising=False)
    monkeypatch.delenv("KEYCLOAK_MINIO_CLIENT_SECRET", raising=False)

    with pytest.raises(ValueError, match="Missing required env vars"):
        verify.build_client_configs("http://localhost:8080")


def test_verify_airflow_redirect_accepts_expected_location(monkeypatch: pytest.MonkeyPatch) -> None:
    @dataclass
    class DummyResponse:
        status_code: int
        headers: dict[str, str]

    def fake_get(*_args, **_kwargs):
        return DummyResponse(
            status_code=302,
            headers={
                "Location": "http://localhost:8090/realms/odp/protocol/openid-connect/auth?client_id=airflow",
            },
        )

    monkeypatch.delenv("KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL", raising=False)
    monkeypatch.setattr(verify.requests, "get", fake_get)

    verify.verify_airflow_redirect(
        airflow_base_url="http://localhost:8080",
        keycloak_base_url="http://localhost:8090",
        keycloak_realm="odp",
        timeout_s=5.0,
    )


def test_verify_clients_checks_token_claims(monkeypatch: pytest.MonkeyPatch) -> None:
    class Metadata:
        jwks_uri = "http://localhost/jwks"

    def fake_fetch_openid_configuration(*_args, **_kwargs):
        return Metadata()

    def fake_fetch_jwks(*_args, **_kwargs):
        return {"keys": [{"kid": "one"}]}

    def fake_fetch_tokens_auth_code_pkce(*_args, **kwargs):
        client_id = kwargs["client_id"]
        return {"access_token": f"token-for-{client_id}"}

    def fake_decode_jwt_claims(token: str):
        client_id = token.removeprefix("token-for-")
        return {"azp": client_id, "iss": "http://localhost:8090/realms/odp"}

    monkeypatch.setattr(verify, "fetch_openid_configuration", fake_fetch_openid_configuration)
    monkeypatch.setattr(verify, "fetch_jwks", fake_fetch_jwks)
    monkeypatch.setattr(verify, "fetch_tokens_auth_code_pkce", fake_fetch_tokens_auth_code_pkce)
    monkeypatch.setattr(verify, "decode_jwt_claims", fake_decode_jwt_claims)

    verify.verify_clients(
        keycloak_base_url="http://localhost:8090",
        keycloak_realm="odp",
        clients=[
            verify.OIDCClientConfig("airflow", "airflow", "secret", "http://localhost:8080/oauth-authorized/keycloak"),
            verify.OIDCClientConfig("datahub", "datahub", "secret", "http://localhost:9002/callback/oidc"),
            verify.OIDCClientConfig("minio", "minio", "secret", "http://localhost:9001/oauth_callback"),
        ],
        username="odp-admin",
        password="admin",
        timeout_s=5.0,
    )


def test_verify_with_retries_retries_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    attempts = {"count": 0}

    def fake_verify_once(*_args, **_kwargs):
        attempts["count"] += 1
        if attempts["count"] < 2:
            raise RuntimeError("not ready yet")

    monkeypatch.setattr(verify, "verify_once", fake_verify_once)
    monkeypatch.setattr(verify.time, "sleep", lambda _delay: None)

    verify.verify_with_retries(retries=3, retry_delay_s=0.01, timeout_s=1.0)
    assert attempts["count"] == 2
