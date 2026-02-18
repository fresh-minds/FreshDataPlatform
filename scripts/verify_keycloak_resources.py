#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import socket
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.sso.oidc import (  # noqa: E402
    OIDCFlowError,
    decode_jwt_claims,
    fetch_jwks,
    fetch_openid_configuration,
    fetch_tokens_auth_code_pkce,
    normalize_keycloak_base_realm,
)


@dataclass(frozen=True)
class OIDCClientConfig:
    name: str
    client_id: str
    client_secret: str
    redirect_uri: str


def _env(name: str, default: str | None = None) -> str | None:
    raw = os.getenv(name)
    if raw is None:
        return default
    stripped = raw.strip()
    return stripped if stripped else default


def _prefer_reachable_localhost(url: str) -> str:
    parsed = urlparse(url)
    host = parsed.hostname
    if not host:
        return url

    if host in {"localhost", "127.0.0.1"}:
        return url

    try:
        socket.gethostbyname(host)
        return url
    except OSError:
        if host != "keycloak":
            return url

    netloc = parsed.netloc.replace(host, "localhost")
    return urlunparse(parsed._replace(netloc=netloc))


def resolve_keycloak_base_and_realm() -> tuple[str, str]:
    oidc_hint = (
        _env("KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL")
        or _env("KEYCLOAK_OIDC_AUTHORIZE_URL")
        or _env("KEYCLOAK_OIDC_BASE_URL")
        or "http://localhost:8090/realms/odp/protocol/openid-connect/auth"
    )
    keycloak_base_url, keycloak_realm = normalize_keycloak_base_realm(
        _env("KEYCLOAK_BASE_URL"),
        _env("KEYCLOAK_REALM"),
        oidc_hint,
    )
    return _prefer_reachable_localhost(keycloak_base_url), keycloak_realm


def build_client_configs(airflow_base_url: str) -> list[OIDCClientConfig]:
    airflow_secret = _env("KEYCLOAK_AIRFLOW_CLIENT_SECRET")
    datahub_secret = _env("KEYCLOAK_DATAHUB_CLIENT_SECRET")
    minio_secret = _env("KEYCLOAK_MINIO_CLIENT_SECRET")

    missing = [
        name
        for name, value in (
            ("KEYCLOAK_AIRFLOW_CLIENT_SECRET", airflow_secret),
            ("KEYCLOAK_DATAHUB_CLIENT_SECRET", datahub_secret),
            ("KEYCLOAK_MINIO_CLIENT_SECRET", minio_secret),
        )
        if not value
    ]
    if missing:
        raise ValueError(f"Missing required env vars for Keycloak client verification: {', '.join(missing)}")

    return [
        OIDCClientConfig(
            name="airflow",
            client_id=_env("KEYCLOAK_AIRFLOW_CLIENT_ID", "airflow") or "airflow",
            client_secret=airflow_secret or "",
            redirect_uri=_env("REDIRECT_URI_AIRFLOW", f"{airflow_base_url.rstrip('/')}/oauth-authorized/keycloak")
            or f"{airflow_base_url.rstrip('/')}/oauth-authorized/keycloak",
        ),
        OIDCClientConfig(
            name="datahub",
            client_id=_env("KEYCLOAK_DATAHUB_CLIENT_ID", "datahub") or "datahub",
            client_secret=datahub_secret or "",
            redirect_uri=_env("REDIRECT_URI_DATAHUB", "http://localhost:9002/callback/oidc")
            or "http://localhost:9002/callback/oidc",
        ),
        OIDCClientConfig(
            name="minio",
            client_id=_env("KEYCLOAK_MINIO_CLIENT_ID", "minio") or "minio",
            client_secret=minio_secret or "",
            redirect_uri=_env("MINIO_OIDC_REDIRECT_URI", "http://localhost:9001/oauth_callback")
            or "http://localhost:9001/oauth_callback",
        ),
    ]


def verify_airflow_redirect(
    *,
    airflow_base_url: str,
    keycloak_base_url: str,
    keycloak_realm: str,
    timeout_s: float,
) -> None:
    base = airflow_base_url.rstrip("/")
    next_url = quote(f"{base}/home", safe="")
    login_url = f"{base}/login/keycloak?next={next_url}"
    response = requests.get(login_url, allow_redirects=False, timeout=timeout_s)

    if response.status_code not in {301, 302, 303, 307, 308}:
        raise RuntimeError(
            f"Airflow Keycloak login did not redirect (status={response.status_code}, url={login_url})",
        )

    location = response.headers.get("Location", "")
    expected_prefix = (
        _env("KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL")
        or f"{keycloak_base_url.rstrip('/')}/realms/{keycloak_realm}/protocol/openid-connect/auth"
    )
    expected_prefix = _prefer_reachable_localhost(expected_prefix.rstrip("/"))

    if not location.startswith(expected_prefix):
        raise RuntimeError(
            "Airflow redirect does not point to browser-reachable Keycloak authorize endpoint "
            f"(expected prefix={expected_prefix}, got={location})",
        )


def verify_clients(
    *,
    keycloak_base_url: str,
    keycloak_realm: str,
    clients: list[OIDCClientConfig],
    username: str,
    password: str,
    timeout_s: float,
) -> None:
    metadata = fetch_openid_configuration(keycloak_base_url, keycloak_realm, timeout_s=timeout_s)
    jwks = fetch_jwks(metadata.jwks_uri, timeout_s=timeout_s)
    if not isinstance(jwks.get("keys"), list) or not jwks["keys"]:
        raise RuntimeError("Keycloak JWKS endpoint returned no keys")

    for client in clients:
        tokens = fetch_tokens_auth_code_pkce(
            keycloak_base_url=keycloak_base_url,
            realm=keycloak_realm,
            client_id=client.client_id,
            client_secret=client.client_secret,
            redirect_uri=client.redirect_uri,
            username=username,
            password=password,
            timeout_s=timeout_s,
        )
        access_token = tokens.get("access_token")
        if not isinstance(access_token, str) or not access_token:
            raise RuntimeError(f"Missing access_token for client '{client.name}'")

        claims = decode_jwt_claims(access_token)
        if claims.get("azp") != client.client_id:
            raise RuntimeError(
                f"Unexpected azp claim for client '{client.name}' (expected={client.client_id}, got={claims.get('azp')})",
            )
        issuer = claims.get("iss")
        if not isinstance(issuer, str) or f"/realms/{keycloak_realm}" not in issuer:
            raise RuntimeError(f"Unexpected issuer claim for client '{client.name}': {issuer}")


def verify_once(timeout_s: float) -> None:
    airflow_base_url = _env("AIRFLOW_BASE_URL", "http://localhost:8080") or "http://localhost:8080"
    basic_username = _env("TEST_USER_BASIC", "odp-admin") or "odp-admin"
    basic_password = _env("TEST_USER_BASIC_PASSWORD", _env("KEYCLOAK_DEFAULT_USER_PASSWORD", "admin")) or "admin"

    keycloak_base_url, keycloak_realm = resolve_keycloak_base_and_realm()
    clients = build_client_configs(airflow_base_url)

    verify_airflow_redirect(
        airflow_base_url=airflow_base_url,
        keycloak_base_url=keycloak_base_url,
        keycloak_realm=keycloak_realm,
        timeout_s=timeout_s,
    )
    verify_clients(
        keycloak_base_url=keycloak_base_url,
        keycloak_realm=keycloak_realm,
        clients=clients,
        username=basic_username,
        password=basic_password,
        timeout_s=timeout_s,
    )


def verify_with_retries(*, retries: int, retry_delay_s: float, timeout_s: float) -> None:
    if retries < 1:
        raise ValueError("--retries must be >= 1")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            verify_once(timeout_s=timeout_s)
            print("[keycloak-verify] Keycloak and OIDC resources verified (airflow, datahub, minio).")
            return
        except (requests.RequestException, OIDCFlowError, ValueError, RuntimeError) as exc:
            last_error = exc
            if attempt == retries:
                break
            print(f"[keycloak-verify] attempt {attempt}/{retries} failed: {exc}")
            time.sleep(retry_delay_s)

    raise RuntimeError(f"Keycloak verification failed after {retries} attempts: {last_error}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Verify Keycloak + OIDC resources used by local bootstrap.")
    parser.add_argument("--timeout", type=float, default=20.0, help="HTTP timeout for each request.")
    parser.add_argument("--retries", type=int, default=30, help="How many verification attempts to make.")
    parser.add_argument("--retry-delay", type=float, default=3.0, help="Seconds to wait between retries.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    repo_root = Path(__file__).resolve().parents[1]
    load_dotenv(repo_root / ".env", override=False)
    try:
        verify_with_retries(retries=args.retries, retry_delay_s=args.retry_delay, timeout_s=args.timeout)
    except Exception as exc:  # noqa: BLE001
        print(f"[keycloak-verify] error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
