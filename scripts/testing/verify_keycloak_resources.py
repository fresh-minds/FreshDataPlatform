#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import socket
import sys
import time
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote, urlparse, urlunparse

import requests
from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[2]
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


def _equivalent_keycloak_authorize_prefixes(url: str) -> tuple[str, ...]:
    parsed = urlparse(url.rstrip("/"))
    host = (parsed.hostname or "").lower()
    port = parsed.port

    if not host:
        return (url.rstrip("/"),)

    equivalent_hosts = {host}
    if host in {"localhost", "127.0.0.1", "keycloak"}:
        equivalent_hosts.update({"localhost", "127.0.0.1", "keycloak"})

    prefixes: list[str] = []
    for candidate_host in sorted(equivalent_hosts):
        netloc_host = candidate_host
        if ":" in candidate_host and not candidate_host.startswith("["):
            netloc_host = f"[{candidate_host}]"

        candidate_netloc = netloc_host
        if port is not None:
            candidate_netloc = f"{netloc_host}:{port}"

        prefixes.append(urlunparse(parsed._replace(netloc=candidate_netloc)).rstrip("/"))

    return tuple(prefixes)


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
    superset_secret = _env("KEYCLOAK_SUPERSET_CLIENT_SECRET", "change_me_keycloak_superset_secret")

    missing = [
        name
        for name, value in (
            ("KEYCLOAK_AIRFLOW_CLIENT_SECRET", airflow_secret),
            ("KEYCLOAK_DATAHUB_CLIENT_SECRET", datahub_secret),
            ("KEYCLOAK_MINIO_CLIENT_SECRET", minio_secret),
            ("KEYCLOAK_SUPERSET_CLIENT_SECRET", superset_secret),
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
        OIDCClientConfig(
            name="superset",
            client_id=_env("KEYCLOAK_SUPERSET_CLIENT_ID", "superset") or "superset",
            client_secret=superset_secret or "",
            redirect_uri=_env(
                "REDIRECT_URI_SUPERSET",
                "http://localhost:8088/oauth-authorized/keycloak",
            )
            or "http://localhost:8088/oauth-authorized/keycloak",
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
    allowed_prefixes = _equivalent_keycloak_authorize_prefixes(expected_prefix)

    if not any(location.startswith(prefix) for prefix in allowed_prefixes):
        raise RuntimeError(
            "Airflow redirect does not point to browser-reachable Keycloak authorize endpoint "
            f"(expected one of={allowed_prefixes}, got={location})",
        )


def verify_superset_redirect(
    *,
    superset_base_url: str,
    keycloak_base_url: str,
    keycloak_realm: str,
    timeout_s: float,
) -> None:
    base = superset_base_url.rstrip("/")
    login_url = f"{base}/login/keycloak"
    response = requests.get(login_url, allow_redirects=False, timeout=timeout_s)

    if response.status_code not in {301, 302, 303, 307, 308}:
        raise RuntimeError(
            f"Superset Keycloak login did not redirect (status={response.status_code}, url={login_url})",
        )

    location = response.headers.get("Location", "")
    expected_prefix = (
        _env("KEYCLOAK_OIDC_SUPERSET_BROWSER_AUTHORIZE_URL")
        or _env("KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL")
        or f"{keycloak_base_url.rstrip('/')}/realms/{keycloak_realm}/protocol/openid-connect/auth"
    )
    expected_prefix = _prefer_reachable_localhost(expected_prefix.rstrip("/"))
    allowed_prefixes = _equivalent_keycloak_authorize_prefixes(expected_prefix)

    if not any(location.startswith(prefix) for prefix in allowed_prefixes):
        raise RuntimeError(
            "Superset redirect does not point to browser-reachable Keycloak authorize endpoint "
            f"(expected one of={allowed_prefixes}, got={location})",
        )


def verify_minio_sso_bridge_start(
    *,
    minio_sso_bridge_url: str,
    keycloak_base_url: str,
    keycloak_realm: str,
    timeout_s: float,
) -> None:
    start_url = f"{minio_sso_bridge_url.rstrip('/')}/start"
    response = requests.get(start_url, allow_redirects=False, timeout=timeout_s)

    if response.status_code not in {301, 302, 303, 307, 308}:
        raise RuntimeError(
            f"MinIO SSO bridge start did not redirect (status={response.status_code}, url={start_url})",
        )

    location = response.headers.get("Location", "")
    expected_prefix = (
        _env("KEYCLOAK_BROWSER_BASE_URL")
        or _env("KEYCLOAK_OIDC_BROWSER_AUTHORIZE_URL")
        or f"{keycloak_base_url.rstrip('/')}/realms/{keycloak_realm}/protocol/openid-connect/auth"
    ).rstrip("/")
    if "/protocol/openid-connect/auth" not in expected_prefix:
        expected_prefix = f"{expected_prefix}/realms/{keycloak_realm}/protocol/openid-connect/auth"

    expected_prefix = _prefer_reachable_localhost(expected_prefix)
    allowed_prefixes = _equivalent_keycloak_authorize_prefixes(expected_prefix)
    if not any(location.startswith(prefix) for prefix in allowed_prefixes):
        raise RuntimeError(
            "MinIO SSO bridge redirect does not point to browser-reachable Keycloak authorize endpoint "
            f"(expected one of={allowed_prefixes}, got={location})",
        )

    set_cookie = response.headers.get("Set-Cookie", "")
    if "minio_sso_bridge_state=" not in set_cookie:
        raise RuntimeError("MinIO SSO bridge start did not set state cookie")


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


def _sts_xml_value(root: ET.Element, tag: str) -> str | None:
    namespaced = root.find(f".//{{https://sts.amazonaws.com/doc/2011-06-15/}}{tag}")
    if namespaced is not None and namespaced.text:
        return namespaced.text
    plain = root.find(f".//{tag}")
    if plain is not None and plain.text:
        return plain.text
    return None


def _extract_sts_error_message(body: str) -> str:
    try:
        root = ET.fromstring(body)
    except ET.ParseError:
        return body.strip()[:500]

    message = _sts_xml_value(root, "Message")
    if message:
        return message
    return body.strip()[:500]


def verify_minio_console_login_via_keycloak(
    *,
    keycloak_base_url: str,
    keycloak_realm: str,
    minio_client: OIDCClientConfig,
    username: str,
    password: str,
    minio_api_url: str,
    minio_console_url: str,
    timeout_s: float,
) -> None:
    token_endpoint = f"{keycloak_base_url.rstrip('/')}/realms/{keycloak_realm}/protocol/openid-connect/token"
    token_response = requests.post(
        token_endpoint,
        data={
            "grant_type": "password",
            "client_id": minio_client.client_id,
            "client_secret": minio_client.client_secret,
            "username": username,
            "password": password,
            "scope": "openid profile email",
        },
        timeout=timeout_s,
    )
    if token_response.status_code != 200:
        raise RuntimeError(
            "Failed to obtain Keycloak token for MinIO verification "
            f"(status={token_response.status_code}, endpoint={token_endpoint})",
        )

    token_payload = token_response.json()
    web_identity_token = token_payload.get("id_token") or token_payload.get("access_token")
    if not isinstance(web_identity_token, str) or not web_identity_token:
        raise RuntimeError("Keycloak token response for MinIO is missing id_token/access_token")

    sts_response = requests.post(
        f"{minio_api_url.rstrip('/')}/",
        params={
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": web_identity_token,
            "DurationSeconds": "900",
        },
        timeout=timeout_s,
    )
    if sts_response.status_code != 200:
        sts_error = _extract_sts_error_message(sts_response.text)
        remediation_hint = (
            "Remediation: ensure MinIO OpenID is in claim-based mode "
            "(MINIO_IDENTITY_OPENID_CLAIM_NAME=policy and empty MINIO_IDENTITY_OPENID_ROLE_POLICY), "
            "then restart MinIO."
        )
        raise RuntimeError(
            "MinIO STS AssumeRoleWithWebIdentity failed "
            f"(status={sts_response.status_code}, error={sts_error}). {remediation_hint}",
        )

    sts_xml = ET.fromstring(sts_response.text)
    access_key = _sts_xml_value(sts_xml, "AccessKeyId")
    secret_key = _sts_xml_value(sts_xml, "SecretAccessKey")
    session_token = _sts_xml_value(sts_xml, "SessionToken")
    if not access_key or not secret_key or not session_token:
        raise RuntimeError("MinIO STS response missing temporary credentials")

    login_response = requests.post(
        f"{minio_console_url.rstrip('/')}/api/v1/login",
        json={"accessKey": access_key, "secretKey": secret_key, "sts": session_token},
        timeout=timeout_s,
    )
    if login_response.status_code != 204:
        raise RuntimeError(
            "MinIO Console login with Keycloak-derived STS credentials failed "
            f"(status={login_response.status_code})",
        )

    session_response = requests.get(
        f"{minio_console_url.rstrip('/')}/api/v1/session",
        cookies=login_response.cookies,
        timeout=timeout_s,
    )
    if session_response.status_code != 200:
        raise RuntimeError(
            "MinIO Console session check failed after Keycloak login "
            f"(status={session_response.status_code})",
        )

    session_json = session_response.json()
    permissions = session_json.get("permissions")
    if not isinstance(permissions, dict) or not permissions:
        raise RuntimeError("MinIO Console session from Keycloak login has no permissions")


def verify_once(timeout_s: float) -> None:
    airflow_base_url = _env("AIRFLOW_BASE_URL", "http://localhost:8080") or "http://localhost:8080"
    superset_base_url = _env("SUPERSET_BASE_URL", "http://localhost:8088") or "http://localhost:8088"
    basic_username = _env("TEST_USER_BASIC", "odp-admin") or "odp-admin"
    basic_password = _env("TEST_USER_BASIC_PASSWORD", _env("KEYCLOAK_DEFAULT_USER_PASSWORD", "admin")) or "admin"
    minio_api_url = _env("MINIO_API_URL", "http://localhost:9000") or "http://localhost:9000"
    minio_console_url = _env("MINIO_CONSOLE_URL", "http://localhost:9001") or "http://localhost:9001"
    minio_sso_bridge_url = _env("MINIO_SSO_BRIDGE_URL", "http://localhost:9011") or "http://localhost:9011"

    keycloak_base_url, keycloak_realm = resolve_keycloak_base_and_realm()
    clients = build_client_configs(airflow_base_url)
    minio_client = next((client for client in clients if client.name == "minio"), None)
    if minio_client is None:
        raise RuntimeError("Missing MinIO OIDC client configuration")

    verify_airflow_redirect(
        airflow_base_url=airflow_base_url,
        keycloak_base_url=keycloak_base_url,
        keycloak_realm=keycloak_realm,
        timeout_s=timeout_s,
    )
    verify_superset_redirect(
        superset_base_url=superset_base_url,
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
    verify_minio_console_login_via_keycloak(
        keycloak_base_url=keycloak_base_url,
        keycloak_realm=keycloak_realm,
        minio_client=minio_client,
        username=basic_username,
        password=basic_password,
        minio_api_url=minio_api_url,
        minio_console_url=minio_console_url,
        timeout_s=timeout_s,
    )
    verify_minio_sso_bridge_start(
        minio_sso_bridge_url=minio_sso_bridge_url,
        keycloak_base_url=keycloak_base_url,
        keycloak_realm=keycloak_realm,
        timeout_s=timeout_s,
    )


def verify_with_retries(*, retries: int, retry_delay_s: float, timeout_s: float) -> None:
    if retries < 1:
        raise ValueError("--retries must be >= 1")

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            verify_once(timeout_s=timeout_s)
            print("[keycloak-verify] Keycloak and OIDC resources verified (airflow, datahub, minio, superset).")
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
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv(repo_root / ".env", override=False)
    try:
        verify_with_retries(retries=args.retries, retry_delay_s=args.retry_delay, timeout_s=args.timeout)
    except Exception as exc:  # noqa: BLE001
        print(f"[keycloak-verify] error: {exc}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
