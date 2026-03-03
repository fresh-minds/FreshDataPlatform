from __future__ import annotations

import logging
import os
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Literal
from urllib.parse import urlsplit
from uuid import uuid4

import jwt
import requests
from azure.ai.projects import AIProjectClient
from azure.core.credentials import TokenCredential
from azure.core.exceptions import HttpResponseError
from azure.identity import ChainedTokenCredential, ClientSecretCredential, DefaultAzureCredential
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from jwt import InvalidTokenError, PyJWK
from pydantic import BaseModel, Field


def _strip_wrapping_quotes(value: str) -> str:
    """Trim outer whitespace and remove one matching wrapping quote pair."""
    stripped = value.strip()
    if len(stripped) >= 2 and (
        (stripped.startswith("'") and stripped.endswith("'"))
        or (stripped.startswith('"') and stripped.endswith('"'))
    ):
        return stripped[1:-1]
    return stripped


def _env(
    name: str,
    default: str | None = None,
    *,
    required: bool = False,
    strip_wrapping_quotes: bool = False,
) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
        if required:
            raise RuntimeError(f"Missing required environment variable: {name}")
        return ""
    if strip_wrapping_quotes:
        value = _strip_wrapping_quotes(value)
        if value == "":
            if required:
                raise RuntimeError(f"Missing required environment variable: {name}")
            return ""
    return value


@dataclass(frozen=True)
class ApiSettings:
    keycloak_internal_base_url: str
    keycloak_realm: str
    keycloak_admin_user: str
    keycloak_admin_password: str
    keycloak_admin_realm: str
    portal_client_id: str
    cors_origins: list[str]
    azure_foundry_agent_endpoint: str
    azure_foundry_agent_id: str
    azure_foundry_agent_name: str
    azure_foundry_api_key: str


SETTINGS = ApiSettings(
    keycloak_internal_base_url=_env("KEYCLOAK_INTERNAL_BASE_URL", "http://keycloak:8090"),
    keycloak_realm=_env("KEYCLOAK_REALM", "odp"),
    keycloak_admin_user=_env("KEYCLOAK_ADMIN_USER", required=True),
    keycloak_admin_password=_env("KEYCLOAK_ADMIN_PASSWORD", required=True),
    keycloak_admin_realm=_env("KEYCLOAK_ADMIN_REALM", "master"),
    portal_client_id=_env("PORTAL_CLIENT_ID", "portal"),
    azure_foundry_agent_endpoint=_env(
        "AZURE_EXISTING_AIPROJECT_ENDPOINT",
        _env("AZURE_FOUNDRY_AGENT_ENDPOINT", "", strip_wrapping_quotes=True),
        strip_wrapping_quotes=True,
    ),
    azure_foundry_agent_id=_env(
        "AZURE_EXISTING_AGENT_ID",
        _env("AZURE_FOUNDRY_AGENT_ID", "", strip_wrapping_quotes=True),
        strip_wrapping_quotes=True,
    ),
    azure_foundry_agent_name=_env(
        "AZURE_EXISTING_AGENT_NAME",
        _env("AZURE_FOUNDRY_AGENT_NAME", "", strip_wrapping_quotes=True),
        strip_wrapping_quotes=True,
    ),
    azure_foundry_api_key=_env("AZURE_FOUNDRY_API_KEY", "", strip_wrapping_quotes=True),
    cors_origins=[
        origin.strip()
        for origin in _env("PORTAL_CORS_ORIGINS", "http://localhost:3000").split(",")
        if origin.strip()
    ],
)

logging.basicConfig(level=_env("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("portal_api")

_foundry_project_client: AIProjectClient | None = None
_foundry_openai_client = None
_foundry_default_credential: TokenCredential | None = None

app = FastAPI(title="Portal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Authorization", "Content-Type"],
)


class ChatMessage(BaseModel):
    role: Literal["user", "assistant"]
    content: str = Field(min_length=1, max_length=4000)


class ChatRequest(BaseModel):
    message: str = Field(min_length=1, max_length=4000)
    history: list[ChatMessage] = Field(default_factory=list)


class ChatResponse(BaseModel):
    reply: str


class HomeVisitRequest(BaseModel):
    page: str = Field(default="/", min_length=1, max_length=256)


class HomeVisitResponse(BaseModel):
    visitId: str
    totalHomeVisits: int


class PageVisitResponse(BaseModel):
    page: str
    pageVisits: int
    totalPageVisits: int


class LoginMetadataRequest(BaseModel):
    visitId: str | None = Field(default=None, max_length=128)


# JWKS cache
_jwks_cache: dict = {"keys": [], "fetched_at": 0.0}
_JWKS_CACHE_TTL_SECONDS = 300

# In-memory telemetry store for anonymous home visits and login metadata.
_visit_lock = threading.Lock()
_home_visit_total = 0
_page_visit_total = 0
_pending_home_visits: dict[str, dict] = {}
_login_events: list[dict] = []
_page_visit_counts: dict[str, int] = {}
_page_visit_unique_ips: dict[str, set[str]] = {}
_api_endpoint_counts: dict[str, int] = {}
_MAX_PENDING_HOME_VISITS = 5000
_MAX_LOGIN_EVENTS = 2000
_MAX_COUNTER_KEYS = 500


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _request_ip(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for", "")
    if forwarded_for:
        return forwarded_for.split(",", 1)[0].strip()
    if request.client and request.client.host:
        return request.client.host
    return ""


def _increment_counter(counter_store: dict[str, int], key: str) -> int:
    if key not in counter_store and len(counter_store) >= _MAX_COUNTER_KEYS:
        oldest_key = next(iter(counter_store), None)
        if oldest_key:
            counter_store.pop(oldest_key, None)
    counter_store[key] = counter_store.get(key, 0) + 1
    return counter_store[key]


def _track_page_visit(page: str, ip_address: str) -> tuple[int, int]:
    global _page_visit_total
    page_value = _normalise_page(page)
    page_ip_set = _page_visit_unique_ips.get(page_value)
    if page_ip_set is None:
        if len(_page_visit_unique_ips) >= _MAX_COUNTER_KEYS:
            oldest_page = next(iter(_page_visit_unique_ips), None)
            if oldest_page:
                _page_visit_unique_ips.pop(oldest_page, None)
                _page_visit_counts.pop(oldest_page, None)
        page_ip_set = set()
        _page_visit_unique_ips[page_value] = page_ip_set

    ip_value = ip_address.strip() or "unknown"
    if ip_value not in page_ip_set:
        page_ip_set.add(ip_value)
        _page_visit_counts[page_value] = len(page_ip_set)
        _page_visit_total += 1

    page_count = _page_visit_counts.get(page_value, 0)
    return page_count, _page_visit_total


def _normalise_page(page: str) -> str:
    raw_value = (page or "").strip()
    if not raw_value:
        return "/"

    parts = urlsplit(raw_value)
    candidate = (parts.path or raw_value).strip()
    if not candidate.startswith("/"):
        candidate = f"/{candidate}"

    return candidate[:256]


@app.middleware("http")
async def _track_api_endpoint_visits(request: Request, call_next):
    response = await call_next(request)
    path = request.url.path
    if path.startswith("/api/") and not path.startswith("/api/admin"):
        endpoint_key = f"{request.method.upper()} {path}"
        with _visit_lock:
            _increment_counter(_api_endpoint_counts, endpoint_key)
    return response


def _jwks_uri() -> str:
    return (
        f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/realms/"
        f"{SETTINGS.keycloak_realm}/protocol/openid-connect/certs"
    )


def _get_jwks() -> list[dict]:
    now = time.time()
    if _jwks_cache["keys"] and now - _jwks_cache["fetched_at"] < _JWKS_CACHE_TTL_SECONDS:
        return _jwks_cache["keys"]

    try:
        response = requests.get(_jwks_uri(), timeout=10)
        response.raise_for_status()
        keys = response.json().get("keys", [])
        _jwks_cache["keys"] = keys
        _jwks_cache["fetched_at"] = now
        return keys
    except Exception:
        LOG.exception("Failed to fetch JWKS from Keycloak")
        return _jwks_cache["keys"]


def _find_key(kid: str) -> dict | None:
    keys = _get_jwks()
    for key in keys:
        if key.get("kid") == kid:
            return key

    # Key not found — force refresh and retry once
    _jwks_cache["fetched_at"] = 0.0
    keys = _get_jwks()
    for key in keys:
        if key.get("kid") == kid:
            return key

    return None


# JWT validation

def _verify_portal_token(token: str) -> dict:
    try:
        unverified_header = jwt.get_unverified_header(token)
    except InvalidTokenError as exc:
        LOG.warning("Invalid token header: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token header") from exc

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Token missing kid")

    key = _find_key(kid)
    if key is None:
        raise HTTPException(status_code=401, detail="Unknown signing key")

    try:
        signing_key = PyJWK.from_dict(key).key
    except Exception as exc:
        LOG.warning("Invalid JWK for token kid=%s", kid)
        raise HTTPException(status_code=401, detail="Invalid signing key") from exc

    try:
        # Decode and verify signature and audience, but allow issuer hostname differences
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=SETTINGS.portal_client_id,
            options={"verify_iss": False},
        )
        # Accept any issuer that ends with the expected realm path to allow host/hostname differences
        expected_suffix = f"/realms/{SETTINGS.keycloak_realm}"
        iss = claims.get("iss", "")
        if not isinstance(iss, str) or not iss.endswith(expected_suffix):
            LOG.warning("Token issuer mismatch: got=%s expected_suffix=%s", iss, expected_suffix)
            raise HTTPException(status_code=401, detail="Invalid issuer")
        return claims
    except InvalidTokenError as exc:
        LOG.warning("JWT validation failed: %s", exc)
        LOG.debug("Expected audience=%s issuerSuffix=%s", SETTINGS.portal_client_id, f"/realms/{SETTINGS.keycloak_realm}")
        raise HTTPException(status_code=401, detail="Token validation failed") from exc


def _extract_bearer_token(request: Request) -> str:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.lower().startswith("bearer "):
        LOG.info("Missing or malformed Authorization header; present=%s", bool(auth_header))
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return auth_header[7:]


def _require_admin(claims: dict) -> None:
    roles = set(((claims.get("realm_access") or {}).get("roles") or []))
    if "admin" not in roles:
        raise HTTPException(status_code=403, detail="Admin role required")


def _claim_username(claims: dict) -> str:
    return claims.get("preferred_username") or claims.get("email") or "unknown"


def _extract_roles(claims: dict) -> list[str]:
    roles = ((claims.get("realm_access") or {}).get("roles") or [])
    return [role for role in roles if isinstance(role, str)]


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


def _fetch_users(admin_token: str) -> list[dict]:
    users_url = (
        f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/admin/realms/"
        f"{SETTINGS.keycloak_realm}/users"
    )

    response = requests.get(
        users_url,
        params={"max": 200},
        headers={"Authorization": f"Bearer {admin_token}"},
        timeout=10,
    )
    if response.status_code != 200:
        LOG.warning("Failed to fetch users: status=%s", response.status_code)
        raise HTTPException(status_code=502, detail="Failed to fetch users from Keycloak")

    raw_users = response.json()

    return [
        {
            "id": u.get("id"),
            "username": u.get("username"),
            "email": u.get("email"),
            "firstName": u.get("firstName"),
            "lastName": u.get("lastName"),
            "createdTimestamp": u.get("createdTimestamp"),
            "enabled": u.get("enabled", True),
        }
        for u in raw_users
        if isinstance(u, dict)
    ]


def _extract_reply_from_responses_payload(data: dict) -> str:
    output_text = data.get("output_text")
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    output = data.get("output") or []
    for item in output:
        if not isinstance(item, dict):
            continue
        content_items = item.get("content") or []
        for content_item in content_items:
            if not isinstance(content_item, dict):
                continue
            text_value = content_item.get("text")
            if isinstance(text_value, str) and text_value.strip():
                return text_value.strip()
            nested_text = content_item.get("content")
            if isinstance(nested_text, str) and nested_text.strip():
                return nested_text.strip()

    return ""


def _foundry_agent_ref() -> dict | None:
    if SETTINGS.azure_foundry_agent_id:
        # Supports values like "agentName:1" and plain names.
        agent_name = SETTINGS.azure_foundry_agent_id.split(":", 1)[0]
        return {"name": agent_name, "type": "agent_reference"}
    if SETTINGS.azure_foundry_agent_name:
        return {"name": SETTINGS.azure_foundry_agent_name, "type": "agent_reference"}
    return None


def _build_foundry_responses_url(base_endpoint: str) -> str:
    endpoint = base_endpoint.rstrip("/")
    if endpoint.endswith("/openai/v1/responses"):
        return endpoint
    return f"{endpoint}/openai/v1/responses"


def _extract_provider_error(response: requests.Response) -> tuple[str, str]:
    provider_code = "provider_error"
    provider_message = response.text[:800] if response.text else "Unknown provider error"

    try:
        data = response.json()
    except ValueError:
        return provider_code, provider_message

    error_obj = data.get("error") if isinstance(data, dict) else None
    if isinstance(error_obj, dict):
        code = error_obj.get("code")
        message = error_obj.get("message")
        if isinstance(code, str) and code.strip():
            provider_code = code.strip()
        if isinstance(message, str) and message.strip():
            provider_message = message.strip()

    return provider_code, provider_message


def _normalise_foundry_input_messages(messages: list[dict]) -> list[dict]:
    return [
        {
            "role": message.get("role", "user"),
            "content": message.get("content", ""),
        }
        for message in messages
    ]


def _raise_foundry_provider_http_exception(claims: dict, status_code: int, provider_message: str) -> None:
    safe_status_code = status_code if 400 <= status_code <= 599 else 502
    LOG.warning(
        "portal_chat_foundry_provider_error subject=%s status=%s code=provider_error body=%s",
        claims.get("sub"),
        safe_status_code,
        provider_message[:800],
    )
    raise HTTPException(
        status_code=safe_status_code,
        detail=f"Foundry error (provider_error): {provider_message}",
    )


def _get_foundry_default_credential() -> TokenCredential:
    global _foundry_default_credential
    if _foundry_default_credential is None:
        tenant_id = _strip_wrapping_quotes(os.getenv("AZURE_TENANT_ID", ""))
        client_id = _strip_wrapping_quotes(os.getenv("AZURE_CLIENT_ID", ""))
        client_secret = _strip_wrapping_quotes(os.getenv("AZURE_CLIENT_SECRET", ""))

        if tenant_id and client_id and client_secret:
            _foundry_default_credential = ChainedTokenCredential(
                ClientSecretCredential(
                    tenant_id=tenant_id,
                    client_id=client_id,
                    client_secret=client_secret,
                ),
                DefaultAzureCredential(
                    exclude_environment_credential=True,
                    exclude_interactive_browser_credential=True,
                ),
            )
        else:
            if any((tenant_id, client_id, client_secret)):
                LOG.warning(
                    "Incomplete Azure service principal credential env vars; "
                    "falling back to non-environment DefaultAzureCredential chain."
                )
            _foundry_default_credential = DefaultAzureCredential(exclude_interactive_browser_credential=True)
    return _foundry_default_credential


def _get_foundry_openai_client():
    global _foundry_project_client, _foundry_openai_client
    if _foundry_openai_client is not None:
        return _foundry_openai_client

    _foundry_project_client = AIProjectClient(
        endpoint=SETTINGS.azure_foundry_agent_endpoint,
        credential=_get_foundry_default_credential(),
    )
    _foundry_openai_client = _foundry_project_client.get_openai_client()
    return _foundry_openai_client


def _extract_reply_from_sdk_response(response_obj: object) -> str:
    output_text = getattr(response_obj, "output_text", None)
    if isinstance(output_text, str) and output_text.strip():
        return output_text.strip()

    model_dump = getattr(response_obj, "model_dump", None)
    if callable(model_dump):
        try:
            data = model_dump()
        except Exception:
            data = {}
        if isinstance(data, dict):
            reply = _extract_reply_from_responses_payload(data)
            if reply:
                return reply

    return ""


def _call_foundry_agent_with_default_credential(messages: list[dict], agent_ref: dict, claims: dict) -> str:
    input_messages = _normalise_foundry_input_messages(messages)

    try:
        openai_client = _get_foundry_openai_client()
        response = openai_client.responses.create(
            input=input_messages,
            extra_body={"agent_reference": agent_ref},
        )
    except HttpResponseError as exc:
        _raise_foundry_provider_http_exception(claims, exc.status_code if isinstance(exc.status_code, int) else 502, str(exc))
    except Exception as exc:
        status_code = getattr(exc, "status_code", None)
        if isinstance(status_code, int) and 400 <= status_code <= 599:
            _raise_foundry_provider_http_exception(claims, status_code, str(exc))
        LOG.exception("portal_chat_foundry_default_credential_failed subject=%s", claims.get("sub"))
        raise HTTPException(
            status_code=502,
            detail="Chat provider authentication failed. Verify credential configuration.",
        ) from exc

    reply = _extract_reply_from_sdk_response(response)
    if not reply:
        raise HTTPException(status_code=502, detail="Chat provider returned empty response")

    return reply


def _call_foundry_agent(messages: list[dict], claims: dict) -> str:
    agent_ref = _foundry_agent_ref()
    if not agent_ref:
        raise HTTPException(status_code=502, detail="Foundry agent is not configured")
    if not SETTINGS.azure_foundry_agent_endpoint:
        raise HTTPException(status_code=502, detail="Foundry agent endpoint missing")
    if not SETTINGS.azure_foundry_api_key:
        return _call_foundry_agent_with_default_credential(messages, agent_ref, claims)

    responses_url = _build_foundry_responses_url(SETTINGS.azure_foundry_agent_endpoint)
    input_messages = _normalise_foundry_input_messages(messages)

    payload = {
        "input": input_messages,
        "agent_reference": agent_ref,
    }

    headers = {
        "Content-Type": "application/json",
        "api-key": SETTINGS.azure_foundry_api_key,
    }

    try:
        response = requests.post(
            responses_url,
            headers=headers,
            json=payload,
            timeout=60,
        )
    except requests.RequestException as exc:
        LOG.exception("portal_chat_foundry_request_failed subject=%s", claims.get("sub"))
        raise HTTPException(status_code=502, detail="Chat provider request failed") from exc

    if response.status_code >= 400:
        provider_code, provider_message = _extract_provider_error(response)
        LOG.warning(
            "portal_chat_foundry_provider_error subject=%s status=%s code=%s body=%s",
            claims.get("sub"),
            response.status_code,
            provider_code,
            response.text[:800],
        )
        raise HTTPException(
            status_code=response.status_code,
            detail=f"Foundry error ({provider_code}): {provider_message}",
        )

    data = response.json()
    reply = _extract_reply_from_responses_payload(data)
    if not reply:
        raise HTTPException(status_code=502, detail="Chat provider returned empty response")

    return reply


@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


@app.post("/api/home-visit", response_model=HomeVisitResponse)
def register_home_visit(request: Request, payload: HomeVisitRequest) -> HomeVisitResponse:
    visit_id = str(uuid4())
    normalised_page = _normalise_page(payload.page)
    event = {
        "visitId": visit_id,
        "page": normalised_page,
        "visitedAt": _now_iso(),
        "userAgent": request.headers.get("user-agent", ""),
        "ipAddress": _request_ip(request),
    }

    with _visit_lock:
        global _home_visit_total
        _home_visit_total += 1
        _track_page_visit(normalised_page, event["ipAddress"])
        _pending_home_visits[visit_id] = event
        if len(_pending_home_visits) > _MAX_PENDING_HOME_VISITS:
            oldest_key = next(iter(_pending_home_visits), None)
            if oldest_key:
                _pending_home_visits.pop(oldest_key, None)
        total = _home_visit_total

    LOG.info("portal_home_visit_registered visit_id=%s page=%s total=%s", visit_id, normalised_page, total)
    return HomeVisitResponse(visitId=visit_id, totalHomeVisits=total)


@app.post("/api/page-visit", response_model=PageVisitResponse)
def register_page_visit(request: Request, payload: HomeVisitRequest) -> PageVisitResponse:
    normalised_page = _normalise_page(payload.page)
    ip_address = _request_ip(request)
    with _visit_lock:
        page_visits, total_page_visits = _track_page_visit(normalised_page, ip_address)

    LOG.info(
        "portal_page_visit_registered page=%s ip=%s page_visits=%s total_page_visits=%s",
        normalised_page,
        ip_address,
        page_visits,
        total_page_visits,
    )
    return PageVisitResponse(page=normalised_page, pageVisits=page_visits, totalPageVisits=total_page_visits)


@app.post("/api/login-metadata")
def register_login_metadata(request: Request, payload: LoginMetadataRequest) -> dict:
    token = _extract_bearer_token(request)
    claims = _verify_portal_token(token)

    visit_event = None
    if payload.visitId:
        with _visit_lock:
            visit_event = _pending_home_visits.pop(payload.visitId, None)

    login_event = {
        "eventId": str(uuid4()),
        "loggedInAt": _now_iso(),
        "subject": claims.get("sub"),
        "username": _claim_username(claims),
        "email": claims.get("email"),
        "roles": _extract_roles(claims),
        "issuer": claims.get("iss"),
        "visitId": payload.visitId,
        "homeVisit": visit_event,
        "userAgent": request.headers.get("user-agent", ""),
        "ipAddress": _request_ip(request),
    }

    with _visit_lock:
        _login_events.append(login_event)
        if len(_login_events) > _MAX_LOGIN_EVENTS:
            del _login_events[: len(_login_events) - _MAX_LOGIN_EVENTS]

    LOG.info(
        "portal_login_metadata_registered subject=%s username=%s visit_id=%s",
        claims.get("sub"),
        _claim_username(claims),
        payload.visitId,
    )
    return {"ok": True}


@app.get("/api/admin/login-metadata")
def get_login_metadata(request: Request) -> dict:
    token = _extract_bearer_token(request)
    claims = _verify_portal_token(token)
    _require_admin(claims)

    with _visit_lock:
        events = list(reversed(_login_events))
        pending = len(_pending_home_visits)
        total_home_visits = _home_visit_total
        total_page_visits = _page_visit_total
        total_api_endpoint_hits = sum(_api_endpoint_counts.values())
        page_visit_counts = [
            {"page": page, "count": count}
            for page, count in sorted(_page_visit_counts.items(), key=lambda item: (-item[1], item[0]))
        ]
        api_endpoint_counts = [
            {"endpoint": endpoint, "count": count}
            for endpoint, count in sorted(_api_endpoint_counts.items(), key=lambda item: (-item[1], item[0]))
        ]

    LOG.info(
        "portal_login_metadata_read subject=%s username=%s total_events=%s total_home_visits=%s",
        claims.get("sub"),
        _claim_username(claims),
        len(events),
        total_home_visits,
    )
    return {
        "totalHomeVisits": total_home_visits,
        "totalPageVisits": total_page_visits,
        "totalApiEndpointHits": total_api_endpoint_hits,
        "pendingHomeVisits": pending,
        "totalLoginEvents": len(events),
        "pageVisitCounts": page_visit_counts,
        "apiEndpointCounts": api_endpoint_counts,
        "loginEvents": events,
    }


@app.delete("/api/admin/login-metadata")
def clear_login_metadata(request: Request) -> dict:
    token = _extract_bearer_token(request)
    claims = _verify_portal_token(token)
    _require_admin(claims)

    with _visit_lock:
        global _home_visit_total, _page_visit_total
        removed_login_events = len(_login_events)
        removed_pending_visits = len(_pending_home_visits)
        removed_page_counters = len(_page_visit_counts)
        removed_endpoint_counters = len(_api_endpoint_counts)
        _login_events.clear()
        _pending_home_visits.clear()
        _page_visit_counts.clear()
        _page_visit_unique_ips.clear()
        _api_endpoint_counts.clear()
        _home_visit_total = 0
        _page_visit_total = 0

    LOG.info(
        "portal_login_metadata_cleared subject=%s username=%s removed_login_events=%s removed_pending_visits=%s",
        claims.get("sub"),
        _claim_username(claims),
        removed_login_events,
        removed_pending_visits,
    )
    return {
        "ok": True,
        "removedLoginEvents": removed_login_events,
        "removedPendingHomeVisits": removed_pending_visits,
        "removedPageCounters": removed_page_counters,
        "removedEndpointCounters": removed_endpoint_counters,
    }


@app.get("/api/users")
def list_users(request: Request) -> dict:
    token = _extract_bearer_token(request)
    claims = _verify_portal_token(token)
    _require_admin(claims)

    admin_token = _keycloak_admin_token()
    if admin_token is None:
        raise HTTPException(status_code=503, detail="Admin authentication unavailable")

    users = _fetch_users(admin_token)
    LOG.info(
        "portal_user_directory_read subject=%s username=%s total=%s",
        claims.get("sub"),
        _claim_username(claims),
        len(users),
    )
    return {"users": users, "total": len(users)}


@app.post("/api/chat", response_model=ChatResponse)
def chat(request: Request, payload: ChatRequest) -> ChatResponse:
    token = _extract_bearer_token(request)
    claims = _verify_portal_token(token)

    messages = [
        {
            "role": item.role,
            "content": item.content,
        }
        for item in payload.history[-12:]
    ]
    messages.append({"role": "user", "content": payload.message})

    LOG.info("portal_chat_provider_mode subject=%s mode=foundry_agent", claims.get("sub"))
    reply = _call_foundry_agent(messages, claims)

    LOG.info(
        "portal_chat_success subject=%s username=%s",
        claims.get("sub"),
        _claim_username(claims),
    )
    return ChatResponse(reply=reply)
