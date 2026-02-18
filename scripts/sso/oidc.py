from __future__ import annotations

import base64
import hashlib
import json
import secrets
import time
from dataclasses import dataclass
from typing import Any
from urllib.parse import parse_qs, urlencode, urljoin, urlparse

import httpx
from bs4 import BeautifulSoup


class OIDCFlowError(RuntimeError):
    """Raised when an OIDC flow step cannot be completed."""


@dataclass(frozen=True)
class OIDCMetadata:
    issuer: str
    authorization_endpoint: str
    token_endpoint: str
    jwks_uri: str
    userinfo_endpoint: str | None
    end_session_endpoint: str | None


def normalize_keycloak_base_realm(
    keycloak_base_url: str | None,
    keycloak_realm: str | None,
    oidc_base_url: str | None = None,
) -> tuple[str, str]:
    """Resolve Keycloak base URL + realm from explicit values or OIDC base URL."""
    base = (keycloak_base_url or "").strip().rstrip("/")
    realm = (keycloak_realm or "").strip()

    if not base and oidc_base_url:
        marker = "/realms/"
        if marker in oidc_base_url:
            prefix, suffix = oidc_base_url.split(marker, 1)
            base = prefix.rstrip("/")
            if not realm:
                realm = suffix.split("/", 1)[0]

    if not base:
        raise ValueError("Keycloak base URL is required")
    if not realm:
        raise ValueError("Keycloak realm is required")

    return base, realm


def fetch_openid_configuration(
    keycloak_base_url: str,
    realm: str,
    timeout_s: float = 20.0,
) -> OIDCMetadata:
    url = f"{keycloak_base_url.rstrip('/')}/realms/{realm}/.well-known/openid-configuration"
    response = httpx.get(url, timeout=timeout_s)
    response.raise_for_status()
    payload = response.json()

    return OIDCMetadata(
        issuer=payload["issuer"],
        authorization_endpoint=payload["authorization_endpoint"],
        token_endpoint=payload["token_endpoint"],
        jwks_uri=payload["jwks_uri"],
        userinfo_endpoint=payload.get("userinfo_endpoint"),
        end_session_endpoint=payload.get("end_session_endpoint"),
    )


def fetch_jwks(jwks_uri: str, timeout_s: float = 20.0) -> dict[str, Any]:
    response = httpx.get(jwks_uri, timeout=timeout_s)
    response.raise_for_status()
    payload = response.json()
    if not isinstance(payload, dict) or "keys" not in payload or not isinstance(payload["keys"], list):
        raise OIDCFlowError("JWKS payload does not contain a valid 'keys' array")
    return payload


def generate_pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)
    challenge = base64.urlsafe_b64encode(hashlib.sha256(verifier.encode("utf-8")).digest()).decode("utf-8").rstrip("=")
    return verifier, challenge


def decode_jwt_claims(token: str) -> dict[str, Any]:
    parts = token.split(".")
    if len(parts) < 2:
        raise ValueError("JWT is malformed")
    payload = parts[1]
    padded = payload + "=" * (-len(payload) % 4)
    claims = json.loads(base64.urlsafe_b64decode(padded).decode("utf-8"))
    if not isinstance(claims, dict):
        raise ValueError("JWT payload is not a JSON object")
    return claims


def extract_authorization_query(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    query: dict[str, str] = {}
    for key, values in parse_qs(parsed.query).items():
        if values:
            query[key] = values[0]
    return query


def _extract_form(response: httpx.Response) -> tuple[str, dict[str, str]]:
    soup = BeautifulSoup(response.text, "html.parser")
    form = soup.find("form", attrs={"id": "kc-form-login"}) or soup.find("form")
    if form is None:
        snippet = response.text[:500].replace("\n", " ")
        raise OIDCFlowError(f"Could not find Keycloak login form. Status={response.status_code}. Snippet={snippet}")

    action = form.get("action")
    if not action:
        raise OIDCFlowError("Keycloak login form is missing an action URL")

    fields: dict[str, str] = {}
    for input_element in form.find_all("input"):
        name = input_element.get("name")
        if not name:
            continue
        fields[name] = input_element.get("value") or ""

    return action, fields


def _extract_code_and_state(redirect_url: str) -> tuple[str, str | None]:
    query = parse_qs(urlparse(redirect_url).query)
    if "error" in query:
        error = query.get("error", [""])[0]
        description = query.get("error_description", [""])[0]
        raise OIDCFlowError(f"Authorization redirect returned error={error} description={description}")

    code = query.get("code", [None])[0]
    state = query.get("state", [None])[0]
    if not code:
        raise OIDCFlowError("Authorization redirect does not contain code parameter")
    return code, state


def _copy_response_cookies_for_http(client: httpx.Client, response: httpx.Response) -> None:
    """Replicate response cookies without Secure requirement for local HTTP test stacks."""
    if response.request.url.scheme != "http":
        return

    for name, value in response.cookies.items():
        client.cookies.set(name, value)


def request_authorization_code(
    *,
    metadata: OIDCMetadata,
    client_id: str,
    redirect_uri: str,
    username: str,
    password: str,
    scope: str = "openid profile email",
    timeout_s: float = 20.0,
    otp_code: str | None = None,
) -> dict[str, str]:
    state = secrets.token_urlsafe(24)
    nonce = secrets.token_urlsafe(24)
    code_verifier, code_challenge = generate_pkce_pair()

    query_params = {
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "response_type": "code",
        "scope": scope,
        "state": state,
        "nonce": nonce,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    with httpx.Client(follow_redirects=False, timeout=timeout_s) as client:
        auth_response = client.get(metadata.authorization_endpoint, params=query_params)
        _copy_response_cookies_for_http(client, auth_response)

        if auth_response.status_code in {301, 302, 303, 307, 308}:
            location = auth_response.headers.get("location", "")
            if location.startswith(redirect_uri):
                code, returned_state = _extract_code_and_state(location)
                return {
                    "code": code,
                    "state": returned_state or "",
                    "expected_state": state,
                    "nonce": nonce,
                    "code_verifier": code_verifier,
                    "auth_url": str(auth_response.request.url),
                }

            if location:
                next_url = urljoin(str(auth_response.request.url), location)
                auth_response = client.get(next_url)
                _copy_response_cookies_for_http(client, auth_response)

        form_action, form_fields = _extract_form(auth_response)
        form_url = urljoin(str(auth_response.request.url), form_action)

        form_fields["username"] = username
        form_fields["password"] = password
        if otp_code:
            if "totp" in form_fields:
                form_fields["totp"] = otp_code
            elif "otp" in form_fields:
                form_fields["otp"] = otp_code

        submit_response = client.post(form_url, data=form_fields)
        _copy_response_cookies_for_http(client, submit_response)
        hops = 0
        while hops < 8:
            hops += 1

            if submit_response.status_code in {301, 302, 303, 307, 308}:
                location = submit_response.headers.get("location", "")
                if not location:
                    raise OIDCFlowError("Redirect response missing Location header")

                absolute_location = urljoin(str(submit_response.request.url), location)
                if absolute_location.startswith(redirect_uri):
                    code, returned_state = _extract_code_and_state(absolute_location)
                    return {
                        "code": code,
                        "state": returned_state or "",
                        "expected_state": state,
                        "nonce": nonce,
                        "code_verifier": code_verifier,
                        "auth_url": str(auth_response.request.url),
                    }

                submit_response = client.get(absolute_location)
                _copy_response_cookies_for_http(client, submit_response)
                continue

            if submit_response.status_code == 200:
                # If we still see the login form, credentials/MFA likely failed.
                if "kc-form-login" in submit_response.text or "name=\"username\"" in submit_response.text:
                    raise OIDCFlowError("Login did not complete; Keycloak login form remained visible")

                # Some flows may return JS redirects; inspect the URL in the response history.
                current_url = str(submit_response.request.url)
                if current_url.startswith(redirect_uri):
                    code, returned_state = _extract_code_and_state(current_url)
                    return {
                        "code": code,
                        "state": returned_state or "",
                        "expected_state": state,
                        "nonce": nonce,
                        "code_verifier": code_verifier,
                        "auth_url": str(auth_response.request.url),
                    }

            break

        snippet = submit_response.text[:500].replace("\n", " ")
        raise OIDCFlowError(
            "Could not obtain authorization code after login. "
            f"Last status={submit_response.status_code}, url={submit_response.request.url}, snippet={snippet}"
        )


def exchange_code_for_tokens(
    *,
    metadata: OIDCMetadata,
    client_id: str,
    redirect_uri: str,
    code: str,
    code_verifier: str,
    client_secret: str | None = None,
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    data: dict[str, str] = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "redirect_uri": redirect_uri,
        "code": code,
        "code_verifier": code_verifier,
    }
    if client_secret:
        data["client_secret"] = client_secret

    response = httpx.post(metadata.token_endpoint, data=data, timeout=timeout_s)
    if response.status_code != 200:
        raise OIDCFlowError(
            f"Token exchange failed ({response.status_code}): {response.text[:400]}"
        )

    payload = response.json()
    if "access_token" not in payload:
        raise OIDCFlowError("Token endpoint response does not include access_token")
    return payload


def refresh_access_token(
    *,
    metadata: OIDCMetadata,
    client_id: str,
    refresh_token: str,
    client_secret: str | None = None,
    timeout_s: float = 20.0,
) -> httpx.Response:
    data: dict[str, str] = {
        "grant_type": "refresh_token",
        "client_id": client_id,
        "refresh_token": refresh_token,
    }
    if client_secret:
        data["client_secret"] = client_secret

    return httpx.post(metadata.token_endpoint, data=data, timeout=timeout_s)


def fetch_tokens_auth_code_pkce(
    *,
    keycloak_base_url: str,
    realm: str,
    client_id: str,
    redirect_uri: str,
    username: str,
    password: str,
    client_secret: str | None = None,
    scope: str = "openid profile email",
    timeout_s: float = 20.0,
    otp_code: str | None = None,
) -> dict[str, Any]:
    metadata = fetch_openid_configuration(keycloak_base_url, realm, timeout_s=timeout_s)
    auth = request_authorization_code(
        metadata=metadata,
        client_id=client_id,
        redirect_uri=redirect_uri,
        username=username,
        password=password,
        scope=scope,
        timeout_s=timeout_s,
        otp_code=otp_code,
    )
    if auth["state"] != auth["expected_state"]:
        raise OIDCFlowError("State mismatch in authorization code redirect")

    tokens = exchange_code_for_tokens(
        metadata=metadata,
        client_id=client_id,
        redirect_uri=redirect_uri,
        code=auth["code"],
        code_verifier=auth["code_verifier"],
        client_secret=client_secret,
        timeout_s=timeout_s,
    )

    id_token = tokens.get("id_token")
    if id_token:
        claims = decode_jwt_claims(id_token)
        token_nonce = claims.get("nonce")
        if token_nonce and token_nonce != auth["nonce"]:
            raise OIDCFlowError("Nonce mismatch in ID token")

    tokens["_meta"] = {
        "auth_url": auth["auth_url"],
        "state": auth["state"],
        "nonce": auth["nonce"],
        "code_verifier": auth["code_verifier"],
    }
    return tokens


def password_grant_token(
    *,
    keycloak_base_url: str,
    realm: str,
    client_id: str,
    username: str,
    password: str,
    client_secret: str | None = None,
    scope: str = "openid profile email",
    timeout_s: float = 20.0,
) -> dict[str, Any]:
    metadata = fetch_openid_configuration(keycloak_base_url, realm, timeout_s=timeout_s)

    data: dict[str, str] = {
        "grant_type": "password",
        "client_id": client_id,
        "username": username,
        "password": password,
        "scope": scope,
    }
    if client_secret:
        data["client_secret"] = client_secret

    response = httpx.post(metadata.token_endpoint, data=data, timeout=timeout_s)
    if response.status_code != 200:
        raise OIDCFlowError(
            f"Password grant failed ({response.status_code}): {response.text[:400]}"
        )
    return response.json()


def current_epoch() -> int:
    return int(time.time())


def token_is_expired(claims: dict[str, Any], *, now: int | None = None, leeway_seconds: int = 0) -> bool:
    now_epoch = current_epoch() if now is None else now
    exp = int(claims.get("exp", 0))
    return now_epoch > (exp + leeway_seconds)


def token_not_yet_valid(claims: dict[str, Any], *, now: int | None = None, leeway_seconds: int = 0) -> bool:
    now_epoch = current_epoch() if now is None else now
    nbf = int(claims.get("nbf", claims.get("iat", 0)))
    return now_epoch + leeway_seconds < nbf


def redact_secret(secret: str | None, keep: int = 4) -> str:
    if not secret:
        return ""
    if len(secret) <= keep:
        return "*" * len(secret)
    return f"{'*' * (len(secret) - keep)}{secret[-keep:]}"


def build_authorize_url(metadata: OIDCMetadata, params: dict[str, str]) -> str:
    return f"{metadata.authorization_endpoint}?{urlencode(params)}"
