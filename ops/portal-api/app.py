from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Literal

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from jose import JWTError, jwt
from pydantic import BaseModel, Field


def _env(name: str, default: str | None = None, *, required: bool = False) -> str:
    value = os.getenv(name, default)
    if value is None or value == "":
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
    azure_foundry_agent_endpoint=_env("AZURE_FOUNDRY_AGENT_ENDPOINT", _env("AZURE_EXISTING_AIPROJECT_ENDPOINT", "")),
    azure_foundry_agent_id=_env("AZURE_FOUNDRY_AGENT_ID", _env("AZURE_EXISTING_AGENT_ID", "")),
    azure_foundry_agent_name=_env("AZURE_FOUNDRY_AGENT_NAME", ""),
    azure_foundry_api_key=_env("AZURE_FOUNDRY_API_KEY", ""),
    cors_origins=[
        origin.strip()
        for origin in _env("PORTAL_CORS_ORIGINS", "http://localhost:3000").split(",")
        if origin.strip()
    ],
)


logging.basicConfig(level=_env("LOG_LEVEL", "INFO").upper())
LOG = logging.getLogger("portal_api")

app = FastAPI(title="Portal API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=SETTINGS.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "OPTIONS"],
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


# JWKS cache
_jwks_cache: dict = {"keys": [], "fetched_at": 0.0}
_JWKS_CACHE_TTL_SECONDS = 300


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
    except JWTError as exc:
        LOG.warning("Invalid token header: %s", exc)
        raise HTTPException(status_code=401, detail="Invalid token header") from exc

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Token missing kid")

    key = _find_key(kid)
    if key is None:
        raise HTTPException(status_code=401, detail="Unknown signing key")

    try:
        # Decode and verify signature and audience, but allow issuer hostname differences
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=SETTINGS.portal_client_id,
            options={"verify_at_hash": False, "verify_iss": False},
        )
        # Accept any issuer that ends with the expected realm path to allow host/hostname differences
        expected_suffix = f"/realms/{SETTINGS.keycloak_realm}"
        iss = claims.get("iss", "")
        if not isinstance(iss, str) or not iss.endswith(expected_suffix):
            LOG.warning("Token issuer mismatch: got=%s expected_suffix=%s", iss, expected_suffix)
            raise HTTPException(status_code=401, detail="Invalid issuer")
        return claims
    except JWTError as exc:
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


def _call_foundry_agent(messages: list[dict], claims: dict) -> str:
    agent_ref = _foundry_agent_ref()
    if not agent_ref:
        raise HTTPException(status_code=502, detail="Foundry agent is not configured")
    if not SETTINGS.azure_foundry_agent_endpoint or not SETTINGS.azure_foundry_api_key:
        raise HTTPException(status_code=502, detail="Foundry agent endpoint or key missing")

    responses_url = _build_foundry_responses_url(SETTINGS.azure_foundry_agent_endpoint)
    input_messages = [
        {
            "role": message.get("role", "user"),
            "content": message.get("content", ""),
        }
        for message in messages
    ]

    payload = {
        "input": input_messages,
        "agent": agent_ref,
    }

    try:
        response = requests.post(
            responses_url,
            headers={
                "api-key": SETTINGS.azure_foundry_api_key,
                "Content-Type": "application/json",
            },
            json=payload,
            timeout=60,
        )
    except requests.RequestException as exc:
        LOG.exception("portal_chat_foundry_request_failed subject=%s", claims.get("sub"))
        raise HTTPException(status_code=502, detail="Chat provider request failed") from exc

    if response.status_code >= 400:
        LOG.warning(
            "portal_chat_foundry_provider_error subject=%s status=%s body=%s",
            claims.get("sub"),
            response.status_code,
            response.text[:800],
        )
        raise HTTPException(status_code=502, detail="Chat provider returned an error")

    data = response.json()
    reply = _extract_reply_from_responses_payload(data)
    if not reply:
        raise HTTPException(status_code=502, detail="Chat provider returned empty response")

    return reply


def _call_openai_chat(messages: list[dict], claims: dict) -> str:
    # Direct OpenAI support removed; keep function present for older callers.
    raise HTTPException(status_code=501, detail="Direct Azure OpenAI support removed; use Foundry agent only")


@app.get("/healthz", response_class=PlainTextResponse)
def healthz() -> str:
    return "ok"


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
        claims.get("preferred_username") or claims.get("email") or "unknown",
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

    # Portal now requires a configured Foundry agent. Fall back removed.
    LOG.info("portal_chat_provider_mode subject=%s mode=foundry_agent", claims.get("sub"))
    reply = _call_foundry_agent(messages, claims)

    LOG.info(
        "portal_chat_success subject=%s username=%s",
        claims.get("sub"),
        claims.get("preferred_username") or claims.get("email") or "unknown",
    )
    return ChatResponse(reply=reply)

