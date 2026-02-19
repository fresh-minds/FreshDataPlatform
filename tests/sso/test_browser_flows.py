from __future__ import annotations

import os
import time
from urllib.parse import urlparse

import pytest

from tests.sso.browser_utils import (
    app_session_cookie_count,
    is_keycloak_auth_url,
    navigate_to_login,
    page_has_keycloak_login_prompt,
    perform_keycloak_login,
    query_params,
    wait_for_app_return,
)
from tests.sso.settings import SSOApp, SSOSettings


def _is_reauth_required(page: object, app: SSOApp, keycloak_base_url: str) -> bool:
    current_url = page.url
    if is_keycloak_auth_url(current_url, keycloak_base_url):
        return page_has_keycloak_login_prompt(page)

    if app.login_trigger_selector:
        try:
            login_trigger = page.locator(app.login_trigger_selector).first
            return login_trigger.is_visible(timeout=1000)
        except Exception:  # noqa: BLE001
            return False

    return "/login" in urlparse(current_url).path


def _pkce_required_for_app(app: SSOApp, sso_settings: SSOSettings) -> bool:
    if not sso_settings.require_pkce:
        return False
    # PKCE is mandatory for public clients and optional for confidential clients.
    return not bool(app.client_secret)


def _browser_sso_apps(sso_settings: SSOSettings) -> list[SSOApp]:
    return [app for app in sso_settings.apps if app.browser_sso]


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


@pytest.mark.e2e
def test_unauthenticated_access_redirects_to_keycloak(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
    write_log: object,
) -> None:
    browser_apps = _browser_sso_apps(sso_settings)
    if not browser_apps:
        pytest.skip("No browser SSO apps configured")

    for app in browser_apps:
        context = browser_context_factory(f"{browser_name}-{app.name}-unauth")
        page = context.new_page()

        auth_url = navigate_to_login(page, app, sso_settings.keycloak_base_url)
        auth_query = query_params(auth_url)

        assert auth_query.get("client_id") == app.client_id, f"{app.name} did not use expected client_id"
        assert auth_query.get("response_type") == "code", f"{app.name} did not use auth code flow"

        if _pkce_required_for_app(app, sso_settings):
            assert auth_query.get("code_challenge"), f"{app.name} auth request missing code_challenge"
            assert auth_query.get("code_challenge_method") == "S256", (
                f"{app.name} auth request missing PKCE S256"
            )

        write_log(f"browser-unauth-{browser_name}-{app.name}", auth_url)


@pytest.mark.e2e
def test_login_establishes_session_for_each_app(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
) -> None:
    browser_apps = _browser_sso_apps(sso_settings)
    if not browser_apps:
        pytest.skip("No browser SSO apps configured")

    for app in browser_apps:
        context = browser_context_factory(f"{browser_name}-{app.name}-login")
        page = context.new_page()

        navigate_to_login(page, app, sso_settings.keycloak_base_url)
        perform_keycloak_login(page, sso_settings.basic_user.username, sso_settings.basic_user.password)
        wait_for_app_return(page, app, sso_settings.keycloak_base_url)

        all_cookies = context.cookies()
        assert any(
            cookie["name"].startswith("KEYCLOAK_")
            or cookie["name"] in {"AUTH_SESSION_ID", "AUTH_SESSION_ID_LEGACY", "KC_AUTH_SESSION_HASH"}
            for cookie in all_cookies
        ), (
            f"{app.name} login did not establish Keycloak session cookie"
        )

        assert app_session_cookie_count(context, app) > 0, f"{app.name} did not establish an app session"


@pytest.mark.e2e
def test_minio_sso_bridge_login_flow(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
) -> None:
    if not _env_bool("SSO_ENABLE_MINIO_BRIDGE", True):
        pytest.skip("MinIO SSO bridge checks are disabled (SSO_ENABLE_MINIO_BRIDGE=false)")

    bridge_url = os.getenv("MINIO_SSO_BRIDGE_BASE_URL", "http://localhost:9011").rstrip("/")
    minio_console_url = os.getenv("MINIO_CONSOLE_PUBLIC_URL", "http://localhost:9001").rstrip("/")
    minio_host = urlparse(minio_console_url).netloc.lower()

    context = browser_context_factory(f"{browser_name}-minio-bridge-login")
    page = context.new_page()

    page.goto(f"{bridge_url}/", wait_until="domcontentloaded")
    page.locator("a:has-text('Sign in with Keycloak')").first.click()

    assert is_keycloak_auth_url(page.url, sso_settings.keycloak_base_url), (
        "MinIO SSO bridge did not redirect to Keycloak authorize endpoint"
    )

    perform_keycloak_login(page, sso_settings.basic_user.username, sso_settings.basic_user.password)

    start = time.time()
    while time.time() - start <= 30:
        current = urlparse(page.url)
        if current.netloc.lower() == minio_host and current.path.startswith("/browser"):
            break
        page.wait_for_timeout(500)
    else:
        raise AssertionError(
            f"MinIO SSO bridge did not land on {minio_console_url}/browser after login; got {page.url}"
        )

    minio_cookies = context.cookies(minio_console_url)
    assert any(cookie["name"] == "token" for cookie in minio_cookies), (
        "MinIO SSO bridge login did not create MinIO console token cookie"
    )


@pytest.mark.e2e
def test_cross_app_sso_uses_existing_session(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
    app_map: dict[str, SSOApp],
) -> None:
    if not sso_settings.cross_app_pairs:
        pytest.skip("No cross-app pairs configured")

    for source_name, target_name in sso_settings.cross_app_pairs:
        source_app = app_map[source_name]
        target_app = app_map[target_name]

        context = browser_context_factory(f"{browser_name}-{source_name}-to-{target_name}-sso")
        page_a = context.new_page()
        navigate_to_login(page_a, source_app, sso_settings.keycloak_base_url)
        perform_keycloak_login(page_a, sso_settings.basic_user.username, sso_settings.basic_user.password)
        wait_for_app_return(page_a, source_app, sso_settings.keycloak_base_url)

        page_b = context.new_page()
        page_b.goto(target_app.base_url, wait_until="domcontentloaded")

        if target_app.login_trigger_selector:
            try:
                login_trigger = page_b.locator(target_app.login_trigger_selector).first
                if login_trigger.is_visible(timeout=1200):
                    login_trigger.click()
            except Exception:  # noqa: BLE001
                pass

        if is_keycloak_auth_url(page_b.url, sso_settings.keycloak_base_url):
            assert not page_has_keycloak_login_prompt(page_b), (
                f"SSO from {source_app.name} to {target_app.name} prompted for credentials"
            )
            wait_for_app_return(page_b, target_app, sso_settings.keycloak_base_url)

        assert app_session_cookie_count(context, target_app) > 0, (
            f"Target app {target_app.name} did not establish authenticated session via SSO"
        )


@pytest.mark.e2e
def test_logout_propagation_matches_expected_design(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
    app_map: dict[str, SSOApp],
) -> None:
    if not sso_settings.cross_app_pairs:
        pytest.skip("No cross-app pairs configured")

    source_name, target_name = sso_settings.cross_app_pairs[0]

    source_app = app_map[source_name]
    target_app = app_map[target_name]

    if not source_app.logout_url:
        pytest.skip(f"Logout URL not configured for source app '{source_app.name}'")

    default_expect_global_logout = "/protocol/openid-connect/logout" in source_app.logout_url
    expect_global_logout = os.getenv(
        "SSO_EXPECT_GLOBAL_LOGOUT",
        "true" if default_expect_global_logout else "false",
    ).lower() in {"1", "true", "yes", "on"}

    context = browser_context_factory(f"{browser_name}-{source_name}-logout-propagation")
    page_a = context.new_page()
    navigate_to_login(page_a, source_app, sso_settings.keycloak_base_url)
    perform_keycloak_login(page_a, sso_settings.basic_user.username, sso_settings.basic_user.password)
    wait_for_app_return(page_a, source_app, sso_settings.keycloak_base_url)

    page_b = context.new_page()
    page_b.goto(target_app.base_url, wait_until="domcontentloaded")
    if target_app.login_trigger_selector:
        try:
            login_trigger = page_b.locator(target_app.login_trigger_selector).first
            if login_trigger.is_visible(timeout=1200):
                login_trigger.click()
        except Exception:  # noqa: BLE001
            pass

    page_a.goto(source_app.logout_url, wait_until="domcontentloaded")
    page_b.goto(target_app.base_url, wait_until="domcontentloaded")

    reauth_required = _is_reauth_required(page_b, target_app, sso_settings.keycloak_base_url)
    if expect_global_logout:
        assert reauth_required, (
            f"Logout in {source_app.name} did not require re-authentication in {target_app.name}"
        )
    else:
        assert not reauth_required, (
            f"Logout in {source_app.name} unexpectedly invalidated session in {target_app.name}"
        )


@pytest.mark.e2e
def test_session_expiry_behavior(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
) -> None:
    if not sso_settings.run_session_expiry_test:
        pytest.skip("Session expiry test disabled (SSO_RUN_SESSION_EXPIRY_TEST=false)")
    if not sso_settings.session_idle_timeout_seconds:
        pytest.skip("Session timeout value missing (SSO_SESSION_IDLE_TIMEOUT_SECONDS)")
    if not sso_settings.apps:
        pytest.skip("No apps configured")

    app = sso_settings.apps[0]
    context = browser_context_factory(f"{browser_name}-{app.name}-session-expiry")
    page = context.new_page()

    navigate_to_login(page, app, sso_settings.keycloak_base_url)
    perform_keycloak_login(page, sso_settings.basic_user.username, sso_settings.basic_user.password)
    wait_for_app_return(page, app, sso_settings.keycloak_base_url)

    time.sleep(sso_settings.session_idle_timeout_seconds + 5)
    page.goto(app.base_url, wait_until="domcontentloaded")

    assert _is_reauth_required(page, app, sso_settings.keycloak_base_url), (
        "Session expiry did not trigger re-authentication"
    )


@pytest.mark.e2e
def test_mfa_prompt_or_completion(
    browser_name: str,
    browser_context_factory: object,
    sso_settings: SSOSettings,
) -> None:
    if not sso_settings.mfa_user:
        pytest.skip("MFA user not configured")

    app = sso_settings.apps[0]
    context = browser_context_factory(f"{browser_name}-{app.name}-mfa")
    page = context.new_page()

    navigate_to_login(page, app, sso_settings.keycloak_base_url)

    perform_keycloak_login(
        page,
        sso_settings.mfa_user.username,
        sso_settings.mfa_user.password,
        otp_code=sso_settings.mfa_user.otp_code,
    )

    if sso_settings.mfa_user.otp_code:
        wait_for_app_return(page, app, sso_settings.keycloak_base_url)
    else:
        otp_locator = page.locator("input[name='totp'], input[name='otp']").first
        assert otp_locator.is_visible(timeout=5000), "MFA user login did not trigger OTP prompt"
