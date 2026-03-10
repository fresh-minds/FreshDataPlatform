from __future__ import annotations

import base64
import hashlib
import hmac
import json
import logging
import os
import secrets
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode, urlsplit

import requests
from defusedxml import ElementTree as ET
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

    @property
    def minio_oauth_callback_url(self) -> str:
        return f"{self.minio_console_public_url.rstrip('/')}/oauth_callback"


SETTINGS = BridgeSettings(
    bridge_base_url=_env("BRIDGE_BASE_URL", "http://localhost:9011"),
    session_secret=_env("MINIO_SSO_BRIDGE_SESSION_SECRET", required=True),
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

if SETTINGS.state_ttl_seconds <= 0:
    raise RuntimeError("MINIO_SSO_BRIDGE_STATE_TTL_SECONDS must be > 0")
if SETTINGS.session_secret.lower().startswith("change_me") or len(SETTINGS.session_secret) < 32:
    raise RuntimeError(
        "MINIO_SSO_BRIDGE_SESSION_SECRET must be a non-placeholder secret with at least 32 characters."
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


def _token_fingerprint(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()[:12]


def _session_probe_result(token: str) -> tuple[bool, int | None]:
    session_url = f"{SETTINGS.minio_console_internal_url.rstrip('/')}/api/v1/session"
    try:
        response = requests.get(
            session_url,
            cookies={"token": token},
            timeout=5,
        )
    except requests.RequestException as exc:
        LOG.warning("Failed to verify MinIO console session cookie: %s", exc)
        return False, None

    return response.status_code == 200, response.status_code


def _url_origin(url: str) -> str:
    parsed = urlsplit(url)
    if parsed.scheme and parsed.netloc:
        return f"{parsed.scheme}://{parsed.netloc}"
    return url.rstrip("/")


def _append_unique(values: list[str], value: str) -> bool:
    if value in values:
        return False
    values.append(value)
    return True


def _minio_client_protocol_mappers() -> list[dict[str, Any]]:
    return [
        {
            "name": "minio-audience",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-audience-mapper",
            "config": {
                "included.client.audience": SETTINGS.keycloak_minio_client_id,
                "access.token.claim": "true",
                "id.token.claim": "false",
                "userinfo.token.claim": "false",
            },
        },
        {
            "name": "minio-policy",
            "protocol": "openid-connect",
            "protocolMapper": "oidc-hardcoded-claim-mapper",
            "config": {
                "claim.name": "policy",
                "claim.value": "consoleAdmin",
                "jsonType.label": "String",
                "access.token.claim": "true",
                "id.token.claim": "true",
                "userinfo.token.claim": "true",
            },
        },
    ]


def _ensure_protocol_mapper(client_doc: dict[str, Any], mapper: dict[str, Any]) -> bool:
    mapper_name = mapper.get("name")
    if not isinstance(mapper_name, str) or not mapper_name:
        return False

    mappers = list(client_doc.get("protocolMappers") or [])
    if any(isinstance(existing, dict) and existing.get("name") == mapper_name for existing in mappers):
        return False

    mappers.append(mapper)
    client_doc["protocolMappers"] = mappers
    return True


def _default_minio_client_doc() -> dict[str, Any]:
    web_origins = list(
        dict.fromkeys(
            (
                _url_origin(SETTINGS.minio_console_public_url),
                _url_origin(SETTINGS.bridge_base_url),
            ),
        ),
    )

    return {
        "clientId": SETTINGS.keycloak_minio_client_id,
        "name": "MinIO Console",
        "enabled": True,
        "protocol": "openid-connect",
        "publicClient": False,
        "secret": SETTINGS.keycloak_minio_client_secret,
        "standardFlowEnabled": True,
        "directAccessGrantsEnabled": True,
        "redirectUris": [
            SETTINGS.minio_oauth_callback_url,
            SETTINGS.callback_url,
        ],
        "webOrigins": web_origins,
        "protocolMappers": _minio_client_protocol_mappers(),
    }


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
        create_response = requests.post(
            clients_url,
            headers=headers,
            json=_default_minio_client_doc(),
            timeout=10,
        )
        if create_response.status_code not in {200, 201, 204}:
            LOG.warning(
                "Failed to create Keycloak client '%s' in realm '%s': status=%s",
                SETTINGS.keycloak_minio_client_id,
                SETTINGS.keycloak_realm,
                create_response.status_code,
            )
            return

        LOG.info(
            "Created missing Keycloak client '%s' in realm '%s'",
            SETTINGS.keycloak_minio_client_id,
            SETTINGS.keycloak_realm,
        )

        response = requests.get(
            clients_url,
            params={"clientId": SETTINGS.keycloak_minio_client_id},
            headers=headers,
            timeout=10,
        )
        if response.status_code != 200:
            LOG.warning(
                "Could not fetch created Keycloak client config for %s",
                SETTINGS.keycloak_minio_client_id,
            )
            return
        clients = response.json()
        if not clients:
            LOG.warning("Keycloak client '%s' still not found after create", SETTINGS.keycloak_minio_client_id)
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

    if client_doc.get("protocol") != "openid-connect":
        client_doc["protocol"] = "openid-connect"
        changed = True

    if client_doc.get("publicClient") is not False:
        client_doc["publicClient"] = False
        changed = True

    if client_doc.get("standardFlowEnabled") is not True:
        client_doc["standardFlowEnabled"] = True
        changed = True

    if client_doc.get("directAccessGrantsEnabled") is not True:
        client_doc["directAccessGrantsEnabled"] = True
        changed = True

    if client_doc.get("secret") != SETTINGS.keycloak_minio_client_secret:
        client_doc["secret"] = SETTINGS.keycloak_minio_client_secret
        changed = True

    redirect_uris = list(client_doc.get("redirectUris") or [])
    changed = _append_unique(redirect_uris, SETTINGS.callback_url) or changed
    changed = _append_unique(redirect_uris, SETTINGS.minio_oauth_callback_url) or changed
    client_doc["redirectUris"] = redirect_uris

    web_origins = list(client_doc.get("webOrigins") or [])
    changed = _append_unique(web_origins, _url_origin(SETTINGS.bridge_base_url)) or changed
    changed = _append_unique(web_origins, _url_origin(SETTINGS.minio_console_public_url)) or changed
    client_doc["webOrigins"] = web_origins

    for mapper in _minio_client_protocol_mappers():
        changed = _ensure_protocol_mapper(client_doc, mapper) or changed

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
def index(request: Request) -> RedirectResponse:
    """Reuse active MinIO console session; otherwise begin Keycloak SSO."""
    token = request.cookies.get("token")
    token_present = bool(token)
    token_fp = _token_fingerprint(token) if token else "-"
    client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "-")

    if token:
        token_valid, probe_status = _session_probe_result(token)
        LOG.info(
            "index request ip=%s token_present=%s token_fp=%s token_valid=%s probe_status=%s",
            client_ip,
            token_present,
            token_fp,
            token_valid,
            probe_status,
        )
    else:
        token_valid = False
        LOG.info(
            "index request ip=%s token_present=%s token_fp=%s token_valid=%s probe_status=%s",
            client_ip,
            token_present,
            token_fp,
            token_valid,
            "-",
        )

    if token_valid:
        return RedirectResponse(
            url=f"{SETTINGS.minio_console_public_url.rstrip('/')}/browser",
            status_code=302,
        )

    return RedirectResponse(url="/start", status_code=302)


@app.get("/start")
@app.get("/start/")
def start_sso(request: Request, interactive: bool = False) -> RedirectResponse:
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    issued_at = int(time.time())

    authorize_url = (
        f"{SETTINGS.keycloak_browser_base_url.rstrip('/')}/realms/{SETTINGS.keycloak_realm}"
        f"/protocol/openid-connect/auth"
    )
    query_params = {
        "response_type": "code",
        "client_id": SETTINGS.keycloak_minio_client_id,
        "redirect_uri": SETTINGS.callback_url,
        "scope": "openid profile email",
        "state": state,
        "nonce": nonce,
    }
    # First attempt is silent session reuse; fallback to interactive login on login_required.
    if not interactive:
        query_params["prompt"] = "none"
    query = urlencode(query_params)

    client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "-")
    LOG.info(
        "start_sso ip=%s interactive=%s realm=%s state_prefix=%s",
        client_ip,
        interactive,
        SETTINGS.keycloak_realm,
        state[:8],
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
def login(request: Request) -> RedirectResponse:
    # Allow direct /login routing without ingress rewrites.
    return start_sso(request)


@app.get("/callback")
@app.get("/callback/")
def callback(request: Request) -> RedirectResponse:
    params = request.query_params
    if params.get("error"):
        error = params.get("error", "")
        state_cookie_present = bool(request.cookies.get(STATE_COOKIE_NAME))
        client_ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "-")
        LOG.warning(
            "callback received error ip=%s error=%s state_cookie_present=%s",
            client_ip,
            error,
            state_cookie_present,
        )
        if error == "login_required":
            # Silent prompt=none failed because no active Keycloak session; proceed interactively.
            response = RedirectResponse(url="/start?interactive=1", status_code=302)
            response.delete_cookie(STATE_COOKIE_NAME, path="/")
            return response

        raise HTTPException(status_code=400, detail=f"Keycloak returned error: {error}")

    code = params.get("code")
    returned_state = params.get("state")
    if not code or not returned_state:
        raise HTTPException(status_code=400, detail="Missing code/state in callback")

    state_cookie = request.cookies.get(STATE_COOKIE_NAME)
    if not state_cookie:
        LOG.warning("callback missing state cookie state_present=%s", bool(returned_state))
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
        LOG.warning("token exchange failed status=%s", token_response.status_code)
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
        LOG.warning("sts exchange failed status=%s detail=%s", sts_response.status_code, detail)
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
        LOG.warning("minio console login failed status=%s", login_response.status_code)
        raise HTTPException(status_code=502, detail="MinIO Console login failed")

    redirect = RedirectResponse(
        url=f"{SETTINGS.minio_console_public_url.rstrip('/')}/browser",
        status_code=302,
    )
    for set_cookie in _extract_set_cookie_headers(login_response):
        redirect.headers.append("set-cookie", set_cookie)

    redirect.delete_cookie(STATE_COOKIE_NAME, path="/")
    return redirect
