from __future__ import annotations

import time
from urllib.parse import parse_qs, urlparse

from tests.sso.settings import SSOApp


def url_host(url: str) -> str:
    return urlparse(url).netloc.lower()


def is_keycloak_auth_url(url: str, keycloak_base_url: str) -> bool:
    parsed = urlparse(url)
    keycloak_host = url_host(keycloak_base_url)
    return parsed.netloc.lower() == keycloak_host and "/protocol/openid-connect/auth" in parsed.path


def query_params(url: str) -> dict[str, str]:
    parsed = urlparse(url)
    values: dict[str, str] = {}
    for key, value in parse_qs(parsed.query).items():
        if value:
            values[key] = value[0]
    return values


def page_has_keycloak_login_prompt(page: object) -> bool:
    selectors = ["#kc-form-login", "input[name='username']", "#username"]
    for selector in selectors:
        try:
            locator = page.locator(selector).first
            if locator.is_visible(timeout=500):
                return True
        except Exception:  # noqa: BLE001
            continue
    return False


def navigate_to_login(page: object, app: SSOApp, keycloak_base_url: str, timeout_seconds: int = 30) -> str:
    page.goto(app.base_url, wait_until="domcontentloaded")

    start = time.time()
    while time.time() - start <= timeout_seconds:
        current_url = page.url
        if is_keycloak_auth_url(current_url, keycloak_base_url):
            return current_url

        if app.login_trigger_selector:
            try:
                trigger = page.locator(app.login_trigger_selector).first
                if trigger.is_visible(timeout=500):
                    trigger.click()
            except Exception:  # noqa: BLE001
                pass

        page.wait_for_timeout(500)

    raise AssertionError(f"{app.name} did not reach Keycloak auth endpoint within {timeout_seconds}s")


def perform_keycloak_login(page: object, username: str, password: str, otp_code: str | None = None) -> None:
    username_input = page.locator("#username, input[name='username']").first
    password_input = page.locator("#password, input[name='password']").first
    submit_button = page.locator("#kc-login, button[type='submit'], input[type='submit']").first

    username_input.wait_for(state="visible", timeout=20000)
    username_input.fill(username)
    password_input.fill(password)

    if otp_code:
        otp_locator = page.locator("input[name='totp'], input[name='otp']").first
        try:
            otp_locator.wait_for(state="visible", timeout=2000)
            otp_locator.fill(otp_code)
        except Exception:  # noqa: BLE001
            pass

    submit_button.click()


def wait_for_app_return(page: object, app: SSOApp, keycloak_base_url: str, timeout_seconds: int = 30) -> None:
    app_host = url_host(app.base_url)
    keycloak_host = url_host(keycloak_base_url)

    start = time.time()
    while time.time() - start <= timeout_seconds:
        current = urlparse(page.url)
        if current.netloc.lower() == app_host and current.netloc.lower() != keycloak_host:
            return
        page.wait_for_timeout(500)

    raise AssertionError(f"Did not return to {app.name} after login within {timeout_seconds}s")


def app_session_cookie_count(context: object, app: SSOApp) -> int:
    try:
        cookies = context.cookies(app.base_url)
    except Exception:  # noqa: BLE001
        return 0
    return len(cookies)
