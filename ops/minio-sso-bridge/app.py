from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import PlainTextResponse, RedirectResponse


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return ""
    return value


@dataclass(frozen=True)
class BridgeSettings:
    bridge_base_url: str
    session_secret: str
    state_ttl_seconds: int
    keycloak_browser_base_url: str
    keycloak_internal_base_url: str
    keycloak_realm: str
    keycloak_minio_client_id: str
    keycloak_minio_client_secret: str
    keycloak_admin_user: str
    keycloak_admin_password: str
    keycloak_admin_realm: str
    minio_console_public_url: str
    minio_console_internal_url: str
    minio_api_internal_url: str

    @property
    def callback_url(self) -> str:
        return f"{self.bridge_base_url.rstrip('/')}/callback"


SETTINGS = BridgeSettings(
    bridge_base_url=_env("BRIDGE_BASE_URL", "http://localhost:9011"),
    session_secret=_env(
        "MINIO_SSO_BRIDGE_SESSION_SECRET",
        "change_me_minio_sso_bridge_session_secret",
    ),
    state_ttl_seconds=int(_env("MINIO_SSO_BRIDGE_STATE_TTL_SECONDS", "300")),
    keycloak_browser_base_url=_env("KEYCLOAK_BROWSER_BASE_URL", "http://localhost:8090"),
    keycloak_internal_base_url=_env("KEYCLOAK_INTERNAL_BASE_URL", "http://keycloak:8090"),
    keycloak_realm=_env("KEYCLOAK_REALM", "odp"),
    keycloak_minio_client_id=_env("KEYCLOAK_MINIO_CLIENT_ID", "minio"),
    keycloak_minio_client_secret=_env("KEYCLOAK_MINIO_CLIENT_SECRET", required=True),
    keycloak_admin_user=_env("KEYCLOAK_ADMIN_USER", ""),
    keycloak_admin_password=_env("KEYCLOAK_ADMIN_PASSWORD", ""),
    keycloak_admin_realm=_env("KEYCLOAK_ADMIN_REALM", "master"),
    minio_console_public_url=_env("MINIO_CONSOLE_PUBLIC_URL", "http://localhost:9001"),
    minio_console_internal_url=_env("MINIO_CONSOLE_INTERNAL_URL", "http://minio:9001"),
    minio_api_internal_url=_env("MINIO_API_INTERNAL_URL", "http://minio:9000"),
)

logging.basicConfig(level=_env("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("minio_sso_bridge")

STATE_COOKIE_NAME = "minio_sso_bridge_state"


@asynccontextmanager
async def lifespan(_: FastAPI):
    try:
        _ensure_minio_client_redirect()
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Skipping Keycloak redirect reconciliation: %s", exc)
    yield


app = FastAPI(title="MinIO SSO Bridge", lifespan=lifespan)


def _sign(payload: str) -> str:
    return hmac.new(SETTINGS.session_secret.encode("utf-8"), payload.encode("utf-8"), hashlib.sha256).hexdigest()


def _make_state_cookie_value(state: str, nonce: str, issued_at: int) -> str:
    payload = f"{state}:{nonce}:{issued_at}"
    signature = _sign(payload)
    return f"{payload}:{signature}"


def _parse_state_cookie(value: str) -> tuple[str, str, int]:
    state, nonce, issued_at, signature = value.split(":", 3)
    payload = f"{state}:{nonce}:{issued_at}"
    if not hmac.compare_digest(_sign(payload), signature):
        raise ValueError("state signature mismatch")

    issued_at_int = int(issued_at)
    if int(time.time()) - issued_at_int > SETTINGS.state_ttl_seconds:
        raise ValueError("state cookie expired")

    return state, nonce, issued_at_int


def _decode_jwt_claims_unverified(token: str) -> dict[str, Any]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload_segment = parts[1]
        payload_segment += "=" * (-len(payload_segment) % 4)
        decoded = base64.urlsafe_b64decode(payload_segment.encode("utf-8")).decode("utf-8")
        payload = json.loads(decoded)
        return payload if isinstance(payload, dict) else {}
    except Exception:  # noqa: BLE001
        return {}


def _sts_xml_value(root: ET.Element, tag: str) -> str | None:
    namespaced = root.find(f".//{{https://sts.amazonaws.com/doc/2011-06-15/}}{tag}")
    if namespaced is not None and namespaced.text:
        return namespaced.text

    plain = root.find(f".//{tag}")
    if plain is not None and plain.text:
        return plain.text

    return None


def _extract_sts_error_message(xml_body: str) -> str:
    try:
        root = ET.fromstring(xml_body)
    except ET.ParseError:
        return xml_body[:500]

    message = _sts_xml_value(root, "Message")
    if message:
        return message

    return xml_body[:500]


def _extract_set_cookie_headers(response: requests.Response) -> list[str]:
    raw_headers = getattr(response.raw, "headers", None)
    if raw_headers is not None and hasattr(raw_headers, "getlist"):
        cookies = [value for value in raw_headers.getlist("Set-Cookie") if value]
        if cookies:
            return cookies

    header_value = response.headers.get("Set-Cookie")
    if header_value:
        return [header_value]

    return []


def _keycloak_admin_token() -> str | None:
    if not SETTINGS.keycloak_admin_user or not SETTINGS.keycloak_admin_password:
        return None

    token_url = (
        f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/realms/"
        f"{SETTINGS.keycloak_admin_realm}/protocol/openid-connect/token"
    )

    response = requests.post(
        token_url,
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": SETTINGS.keycloak_admin_user,
            "password": SETTINGS.keycloak_admin_password,
        },
        timeout=10,
    )
    if response.status_code != 200:
        LOG.warning("Could not fetch Keycloak admin token: status=%s", response.status_code)
        return None

    return response.json().get("access_token")


def _ensure_minio_client_redirect() -> None:
    admin_token = _keycloak_admin_token()
    if not admin_token:
        return

    headers = {"Authorization": f"Bearer {admin_token}", "Content-Type": "application/json"}
    clients_url = (
        f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/admin/realms/"
        f"{SETTINGS.keycloak_realm}/clients"
    )

    response = requests.get(
        clients_url,
        params={"clientId": SETTINGS.keycloak_minio_client_id},
        headers=headers,
        timeout=10,
    )
    if response.status_code != 200:
        LOG.warning("Could not fetch Keycloak client config for %s", SETTINGS.keycloak_minio_client_id)
        return

    clients = response.json()
    if not clients:
        LOG.warning("Keycloak client '%s' not found", SETTINGS.keycloak_minio_client_id)
        return

    client_uuid = clients[0].get("id")
    if not client_uuid:
        LOG.warning("Keycloak client '%s' has no id", SETTINGS.keycloak_minio_client_id)
        return

    detail_url = f"{clients_url}/{client_uuid}"
    detail_response = requests.get(detail_url, headers=headers, timeout=10)
    if detail_response.status_code != 200:
        LOG.warning("Could not fetch Keycloak client details for '%s'", SETTINGS.keycloak_minio_client_id)
        return

    client_doc: dict[str, Any] = detail_response.json()

    changed = False

    redirect_uris = list(client_doc.get("redirectUris") or [])
    if SETTINGS.callback_url not in redirect_uris:
        redirect_uris.append(SETTINGS.callback_url)
        client_doc["redirectUris"] = redirect_uris
        changed = True

    web_origins = list(client_doc.get("webOrigins") or [])
    if SETTINGS.bridge_base_url not in web_origins:
        web_origins.append(SETTINGS.bridge_base_url)
        client_doc["webOrigins"] = web_origins
        changed = True

    if not changed:
        return

    update_response = requests.put(detail_url, headers=headers, json=client_doc, timeout=10)
    if update_response.status_code not in {200, 204}:
        LOG.warning("Failed to update Keycloak client '%s' for bridge redirect", SETTINGS.keycloak_minio_client_id)
        return

    LOG.info("Keycloak client '%s' updated with bridge callback URI", SETTINGS.keycloak_minio_client_id)


@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


@app.get("/")
def index() -> RedirectResponse:
    """Auto-redirect to /start to begin Keycloak SSO immediately."""
    return RedirectResponse(url="/start", status_code=302)


@app.get("/start")
@app.get("/start/")
def start_sso() -> RedirectResponse:
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    issued_at = int(time.time())

    authorize_url = (
        f"{SETTINGS.keycloak_browser_base_url.rstrip('/')}/realms/{SETTINGS.keycloak_realm}"
        f"/protocol/openid-connect/auth"
    )
    query = urlencode(
        {
            "response_type": "code",
            "client_id": SETTINGS.keycloak_minio_client_id,
            "redirect_uri": SETTINGS.callback_url,
            "scope": "openid profile email",
            "state": state,
            "nonce": nonce,
        },
    )

    response = RedirectResponse(url=f"{authorize_url}?{query}", status_code=302)
    response.set_cookie(
        key=STATE_COOKIE_NAME,
        value=_make_state_cookie_value(state, nonce, issued_at),
        max_age=SETTINGS.state_ttl_seconds,
        httponly=True,
        secure=SETTINGS.bridge_base_url.lower().startswith("https://"),
        samesite="lax",
    )
    return response


@app.get("/login")
@app.get("/login/")
def login() -> RedirectResponse:
    # Allow direct /login routing without ingress rewrites.
    return start_sso()


@app.get("/callback")
@app.get("/callback/")
def callback(request: Request) -> RedirectResponse:
    params = request.query_params
    if params.get("error"):
        raise HTTPException(status_code=400, detail=f"Keycloak returned error: {params.get('error')}")

    code = params.get("code")
    returned_state = params.get("state")
    if not code or not returned_state:
        raise HTTPException(status_code=400, detail="Missing code/state in callback")

    state_cookie = request.cookies.get(STATE_COOKIE_NAME)
    if not state_cookie:
        raise HTTPException(status_code=400, detail="Missing state cookie")

    try:
        expected_state, expected_nonce, _ = _parse_state_cookie(state_cookie)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc

    if not hmac.compare_digest(expected_state, returned_state):
        raise HTTPException(status_code=400, detail="State mismatch")

    token_url = (
        f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/realms/{SETTINGS.keycloak_realm}"
        "/protocol/openid-connect/token"
    )
    token_response = requests.post(
        token_url,
        data={
            "grant_type": "authorization_code",
            "client_id": SETTINGS.keycloak_minio_client_id,
            "client_secret": SETTINGS.keycloak_minio_client_secret,
            "code": code,
            "redirect_uri": SETTINGS.callback_url,
        },
        timeout=15,
    )
    if token_response.status_code != 200:
        raise HTTPException(status_code=502, detail="Failed to exchange code with Keycloak")

    token_payload = token_response.json()
    id_token = token_payload.get("id_token")
    if isinstance(id_token, str) and id_token:
        id_token_claims = _decode_jwt_claims_unverified(id_token)
        nonce_claim = id_token_claims.get("nonce")
        if not isinstance(nonce_claim, str) or not hmac.compare_digest(nonce_claim, expected_nonce):
            raise HTTPException(status_code=400, detail="Nonce mismatch")

    web_identity_token = id_token or token_payload.get("access_token")
    if not isinstance(web_identity_token, str) or not web_identity_token:
        raise HTTPException(status_code=502, detail="Keycloak token response missing id/access token")

    sts_response = requests.post(
        f"{SETTINGS.minio_api_internal_url.rstrip('/')}/",
        params={
            "Action": "AssumeRoleWithWebIdentity",
            "Version": "2011-06-15",
            "WebIdentityToken": web_identity_token,
            "DurationSeconds": "900",
        },
        timeout=15,
    )
    if sts_response.status_code != 200:
        detail = _extract_sts_error_message(sts_response.text)
        raise HTTPException(status_code=502, detail=f"MinIO STS failure: {detail}")

    sts_xml = ET.fromstring(sts_response.text)
    access_key = _sts_xml_value(sts_xml, "AccessKeyId")
    secret_key = _sts_xml_value(sts_xml, "SecretAccessKey")
    session_token = _sts_xml_value(sts_xml, "SessionToken")
    if not access_key or not secret_key or not session_token:
        raise HTTPException(status_code=502, detail="MinIO STS response missing credentials")

    login_response = requests.post(
        f"{SETTINGS.minio_console_internal_url.rstrip('/')}/api/v1/login",
        json={
            "accessKey": access_key,
            "secretKey": secret_key,
            "sts": session_token,
        },
        allow_redirects=False,
        timeout=15,
    )
    if login_response.status_code != 204:
        raise HTTPException(status_code=502, detail="MinIO Console login failed")

    redirect = RedirectResponse(
        url=f"{SETTINGS.minio_console_public_url.rstrip('/')}/browser",
        status_code=302,
    )
    for set_cookie in _extract_set_cookie_headers(login_response):
        redirect.headers.append("set-cookie", set_cookie)

    redirect.delete_cookie(STATE_COOKIE_NAME, path="/")
    return redirect
