from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass

import requests
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import PlainTextResponse
from jose import JWTError, jwt
from jose.utils import base64url_decode


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


SETTINGS = ApiSettings(
    keycloak_internal_base_url=_env("KEYCLOAK_INTERNAL_BASE_URL", "http://keycloak:8090"),
    keycloak_realm=_env("KEYCLOAK_REALM", "odp"),
    keycloak_admin_user=_env("KEYCLOAK_ADMIN_USER", required=True),
    keycloak_admin_password=_env("KEYCLOAK_ADMIN_PASSWORD", required=True),
    keycloak_admin_realm=_env("KEYCLOAK_ADMIN_REALM", "master"),
    portal_client_id=_env("PORTAL_CLIENT_ID", "portal"),
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
    allow_methods=["GET"],
    allow_headers=["Authorization", "Content-Type"],
)

# ---------------------------------------------------------------------------
# JWKS cache
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# JWT validation
# ---------------------------------------------------------------------------

def _issuer() -> str:
    return f"{SETTINGS.keycloak_internal_base_url.rstrip('/')}/realms/{SETTINGS.keycloak_realm}"


def _verify_portal_token(token: str) -> dict:
    try:
        unverified_header = jwt.get_unverified_header(token)
    except JWTError as exc:
        raise HTTPException(status_code=401, detail="Invalid token header") from exc

    kid = unverified_header.get("kid")
    if not kid:
        raise HTTPException(status_code=401, detail="Token missing kid")

    key = _find_key(kid)
    if key is None:
        raise HTTPException(status_code=401, detail="Unknown signing key")

    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            audience=SETTINGS.portal_client_id,
            issuer=_issuer(),
            options={"verify_at_hash": False},
        )
        return claims
    except JWTError as exc:
        LOG.warning("JWT validation failed: %s", exc)
        raise HTTPException(status_code=401, detail="Token validation failed") from exc


def _extract_bearer_token(request: Request) -> str:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=401, detail="Missing Bearer token")
    return auth_header[7:]


def _require_admin(claims: dict) -> None:
    roles = set(((claims.get("realm_access") or {}).get("roles") or []))
    if "admin" not in roles:
        raise HTTPException(status_code=403, detail="Admin role required")


# ---------------------------------------------------------------------------
# Keycloak Admin API
# ---------------------------------------------------------------------------

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


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

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
