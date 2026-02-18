from __future__ import annotations

import json
import os
import socket
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from urllib.parse import urlparse, urlunparse

from dotenv import load_dotenv

from scripts.sso.oidc import normalize_keycloak_base_realm


@dataclass(frozen=True)
class SSOUser:
    username: str
    password: str
    otp_code: str | None = None


@dataclass(frozen=True)
class SSOApp:
    name: str
    base_url: str
    client_id: str
    client_secret: str | None
    redirect_uri: str
    login_trigger_selector: str | None
    logout_url: str | None
    enabled: bool = True
    browser_sso: bool = True


@dataclass(frozen=True)
class ProtectedApiExpectation:
    name: str
    url: str
    success_status: int
    missing_role_status: int | None = None


@dataclass(frozen=True)
class SSOSettings:
    repo_root: Path
    artifact_root: Path
    keycloak_base_url: str
    keycloak_realms: tuple[str, ...]
    keycloak_primary_realm: str
    keycloak_admin_user: str | None
    keycloak_admin_password: str | None
    basic_user: SSOUser
    admin_user: SSOUser | None
    no_access_user: SSOUser | None
    mfa_user: SSOUser | None
    apps: tuple[SSOApp, ...]
    browser_names: tuple[str, ...]
    cross_app_pairs: tuple[tuple[str, str], ...]
    require_pkce: bool
    require_https: bool
    expected_refresh_rotation: bool | None
    expected_groups: tuple[str, ...]
    expected_realm_roles: tuple[str, ...]
    expected_resource_roles: tuple[tuple[str, str], ...]
    protected_apis: tuple[ProtectedApiExpectation, ...]
    realm_import_path: Path
    session_idle_timeout_seconds: int | None
    run_session_expiry_test: bool


def _env(name: str, default: str | None = None) -> str | None:
    value = os.getenv(name)
    if value is None:
        return default
    stripped = value.strip()
    return stripped if stripped else default


def _env_bool(name: str, default: bool) -> bool:
    value = _env(name)
    if value is None:
        return default
    return value.lower() in {"1", "true", "yes", "on"}


def _env_optional_bool(name: str) -> bool | None:
    value = _env(name)
    if value is None:
        return None
    lowered = value.lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    return None


def _split_csv(value: str | None) -> tuple[str, ...]:
    if not value:
        return tuple()
    return tuple(item.strip() for item in value.split(",") if item.strip())


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


def _build_app(
    *,
    name: str,
    enabled_default: bool,
    browser_sso_default: bool,
    default_url: str,
    default_client_id: str,
    default_redirect_path: str,
    default_login_selector: str | None,
    default_logout_path: str | None,
) -> SSOApp:
    upper = name.upper()
    base_url = _env(f"APP_{upper}_URL", _env(f"{upper}_URL", default_url))
    assert base_url is not None
    base_url = base_url.rstrip("/")

    client_id = _env(f"CLIENT_ID_{upper}", _env(f"KEYCLOAK_{upper}_CLIENT_ID", default_client_id))
    client_secret = _env(f"CLIENT_SECRET_{upper}", _env(f"KEYCLOAK_{upper}_CLIENT_SECRET"))

    redirect_uri = _env(f"REDIRECT_URI_{upper}", f"{base_url}{default_redirect_path}")
    login_selector = _env(f"LOGIN_SELECTOR_{upper}", default_login_selector)

    logout_override = _env(f"LOGOUT_URL_{upper}")
    if logout_override is not None:
        logout_url = logout_override
    elif default_logout_path is not None:
        logout_url = f"{base_url}{default_logout_path}"
    else:
        logout_url = None

    enabled = _env_bool(f"SSO_ENABLE_{upper}", enabled_default)
    browser_sso = _env_bool(f"SSO_BROWSER_SSO_{upper}", browser_sso_default)

    return SSOApp(
        name=name,
        base_url=base_url,
        client_id=client_id or default_client_id,
        client_secret=client_secret,
        redirect_uri=redirect_uri or f"{base_url}{default_redirect_path}",
        login_trigger_selector=login_selector,
        logout_url=logout_url,
        enabled=enabled,
        browser_sso=browser_sso,
    )


def _parse_expected_resource_roles(raw_roles: tuple[str, ...]) -> tuple[tuple[str, str], ...]:
    parsed: list[tuple[str, str]] = []
    for raw in raw_roles:
        if ":" not in raw:
            continue
        client_id, role = raw.split(":", 1)
        client_id = client_id.strip()
        role = role.strip()
        if client_id and role:
            parsed.append((client_id, role))
    return tuple(parsed)


def _parse_protected_api_matrix() -> tuple[ProtectedApiExpectation, ...]:
    raw = _env("SSO_API_MATRIX_JSON")
    if not raw:
        return tuple()

    payload = json.loads(raw)
    if not isinstance(payload, list):
        raise ValueError("SSO_API_MATRIX_JSON must contain a JSON array")

    expectations: list[ProtectedApiExpectation] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        name = str(item.get("name", "unnamed-api"))
        url = str(item.get("url", "")).strip()
        if not url:
            continue
        success_status = int(item.get("success_status", 200))
        missing_role_raw = item.get("missing_role_status")
        missing_role_status = int(missing_role_raw) if missing_role_raw is not None else None
        expectations.append(
            ProtectedApiExpectation(
                name=name,
                url=url,
                success_status=success_status,
                missing_role_status=missing_role_status,
            )
        )

    return tuple(expectations)


def _default_artifact_root(repo_root: Path) -> Path:
    custom = _env("SSO_ARTIFACT_ROOT")
    if custom:
        return Path(custom).expanduser().resolve()
    return (repo_root / "tests" / "sso" / "artifacts").resolve()


def _parse_cross_app_pairs(raw_pairs: tuple[str, ...], app_names: set[str]) -> tuple[tuple[str, str], ...]:
    if raw_pairs:
        parsed: list[tuple[str, str]] = []
        for pair in raw_pairs:
            if ":" not in pair:
                continue
            left, right = pair.split(":", 1)
            left = left.strip().lower()
            right = right.strip().lower()
            if left in app_names and right in app_names and left != right:
                parsed.append((left, right))
        return tuple(parsed)

    defaults: list[tuple[str, str]] = []
    if "airflow" in app_names and "minio" in app_names:
        defaults.append(("airflow", "minio"))
    if "airflow" in app_names and "datahub" in app_names:
        defaults.append(("airflow", "datahub"))
    return tuple(defaults)


@lru_cache(maxsize=1)
def load_sso_settings() -> SSOSettings:
    repo_root = Path(__file__).resolve().parents[2]
    load_dotenv(repo_root / ".env", override=False)

    keycloak_base_url, realm_from_base = normalize_keycloak_base_realm(
        _env("KEYCLOAK_BASE_URL"),
        _env("KEYCLOAK_REALM"),
        _env("KEYCLOAK_OIDC_BASE_URL"),
    )
    keycloak_base_url = _prefer_reachable_localhost(keycloak_base_url)

    primary_realm = _env("KEYCLOAK_REALM", realm_from_base) or realm_from_base
    realms = _split_csv(_env("KEYCLOAK_REALMS", primary_realm))

    basic_username = _env("TEST_USER_BASIC", "odp-admin") or "odp-admin"
    basic_password = _env("TEST_USER_BASIC_PASSWORD", _env("KEYCLOAK_DEFAULT_USER_PASSWORD", "admin")) or "admin"
    basic_user = SSOUser(username=basic_username, password=basic_password)

    admin_username = _env("TEST_USER_ADMIN")
    admin_password = _env("TEST_USER_ADMIN_PASSWORD")
    if admin_username and admin_password:
        admin_user: SSOUser | None = SSOUser(username=admin_username, password=admin_password)
    else:
        admin_user = None

    no_access_username = _env("TEST_USER_NO_ACCESS")
    no_access_password = _env("TEST_USER_NO_ACCESS_PASSWORD")
    if no_access_username and no_access_password:
        no_access_user: SSOUser | None = SSOUser(username=no_access_username, password=no_access_password)
    else:
        no_access_user = None

    mfa_username = _env("TEST_USER_MFA")
    mfa_password = _env("TEST_USER_MFA_PASSWORD")
    if mfa_username and mfa_password:
        mfa_user: SSOUser | None = SSOUser(
            username=mfa_username,
            password=mfa_password,
            otp_code=_env("TEST_USER_MFA_OTP"),
        )
    else:
        mfa_user = None

    apps = (
        _build_app(
            name="airflow",
            enabled_default=True,
            browser_sso_default=True,
            default_url="http://localhost:8080",
            default_client_id="airflow",
            default_redirect_path="/oauth-authorized/keycloak",
            default_login_selector="a:has-text('keycloak'), button:has-text('keycloak')",
            default_logout_path="/logout/",
        ),
        _build_app(
            name="datahub",
            enabled_default=True,
            browser_sso_default=True,
            default_url="http://localhost:9002",
            default_client_id="datahub",
            default_redirect_path="/callback/oidc",
            default_login_selector=None,
            default_logout_path="/logout",
        ),
        _build_app(
            name="minio",
            enabled_default=True,
            browser_sso_default=False,
            default_url="http://localhost:9001",
            default_client_id="minio",
            default_redirect_path="/oauth_callback",
            default_login_selector="button:has-text('OpenID'), a:has-text('OpenID')",
            default_logout_path=None,
        ),
    )

    enabled_apps = tuple(app for app in apps if app.enabled)
    browser_sso_app_names = {app.name for app in enabled_apps if app.browser_sso}

    browser_names = _split_csv(_env("SSO_BROWSERS", "chromium")) or ("chromium",)
    cross_app_pairs = _parse_cross_app_pairs(
        _split_csv(_env("SSO_CROSS_APP_PAIRS")),
        browser_sso_app_names,
    )

    expected_groups = _split_csv(_env("EXPECTED_GROUPS"))
    expected_realm_roles = _split_csv(_env("EXPECTED_REALM_ROLES"))
    expected_resource_roles = _parse_expected_resource_roles(_split_csv(_env("EXPECTED_RESOURCE_ROLES")))

    protected_apis = _parse_protected_api_matrix()

    realm_import_default = repo_root / "ops" / "keycloak" / "odp-realm.json"
    realm_import_path = Path(_env("KEYCLOAK_REALM_IMPORT_PATH", str(realm_import_default)) or str(realm_import_default))

    session_idle_raw = _env("SSO_SESSION_IDLE_TIMEOUT_SECONDS")
    session_idle_timeout_seconds = int(session_idle_raw) if session_idle_raw else None

    return SSOSettings(
        repo_root=repo_root,
        artifact_root=_default_artifact_root(repo_root),
        keycloak_base_url=keycloak_base_url,
        keycloak_realms=realms,
        keycloak_primary_realm=primary_realm,
        keycloak_admin_user=_env("KEYCLOAK_ADMIN_USER"),
        keycloak_admin_password=_env("KEYCLOAK_ADMIN_PASSWORD"),
        basic_user=basic_user,
        admin_user=admin_user,
        no_access_user=no_access_user,
        mfa_user=mfa_user,
        apps=enabled_apps,
        browser_names=browser_names,
        cross_app_pairs=cross_app_pairs,
        require_pkce=_env_bool("SSO_REQUIRE_PKCE", True),
        require_https=_env_bool("SSO_REQUIRE_HTTPS", False),
        expected_refresh_rotation=_env_optional_bool("SSO_EXPECT_REFRESH_ROTATION"),
        expected_groups=expected_groups,
        expected_realm_roles=expected_realm_roles,
        expected_resource_roles=expected_resource_roles,
        protected_apis=protected_apis,
        realm_import_path=realm_import_path,
        session_idle_timeout_seconds=session_idle_timeout_seconds,
        run_session_expiry_test=_env_bool("SSO_RUN_SESSION_EXPIRY_TEST", False),
    )
