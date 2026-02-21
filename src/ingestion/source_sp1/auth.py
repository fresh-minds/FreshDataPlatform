"""Playwright-based authentication for the Source SP1 portal.

The Source SP1 portal is built on Salesforce Experience Cloud (formerly
Salesforce Communities). The /s/ path prefix is the standard routing prefix
for Salesforce Experience Cloud sites.

Login flow:
  1. Navigate to base_url (unauthenticated sessions are redirected to login).
  2. Locate the username / password form fields (standard Salesforce selectors).
  3. Submit credentials and wait for the post-login navigation bar to appear.
  4. Validate success by the absence of the login form and presence of SF nav.

Credentials are resolved in this priority order:
  1. Airflow connection  ``source_sp1`` (login / password / extra.base_url)
  2. Environment variables:
       SP1_USERNAME
       SP1_PASSWORD
       SP1_BASE_URL  (optional, defaults to SP1_BASE_URL env var)

MFA note: if the portal enforces MFA (e.g. Salesforce Authenticator), the
automated flow will stall after credential submission. In that case, run the
DAG with headless=False locally to complete the MFA step, then export the
Playwright storage state (see README_source_sp1.md § MFA fallback).
"""
from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

# Base URL is loaded from environment — no hardcoded portal URL in source code.
BASE_URL = os.environ.get("SP1_BASE_URL", "")
_LOGIN_TIMEOUT_MS = 30_000

# Salesforce Experience Cloud post-login element selectors (broad set)
_POST_LOGIN_SELECTORS = [
    "nav.slds-context-bar",            # Lightning navigation bar
    ".slds-page-header",               # Lightning page header
    ".comm-navigation",                # Community navigation component
    ".forceCommunityGlobalNavigation", # SF Community global nav
    ".slds-utility-bar",               # Utility bar (SF internal apps)
    ".slds-global-header",             # Global header
    "c-navigation",                    # Custom LWC navigation
    ".navigation-menu",                # Generic nav menu class
    "[data-id='navBar']",              # NavBar data attribute
]

# Possible username field selectors (ordered most → least specific)
_USERNAME_SELECTORS = [
    "#username",
    "input[name='username']",
    "input[autocomplete='username']",
    "input[type='email'][autocomplete]",
    "input[type='email']",
    "input[name='email']",
]

# Possible password field selectors
_PASSWORD_SELECTORS = [
    "#password",
    "input[name='password']",
    "input[autocomplete='current-password']",
    "input[type='password']",
]

# Possible submit button selectors
_SUBMIT_SELECTORS = [
    "#Login",
    "input[id='Login']",
    "input[type='submit']",
    "button[type='submit']",
    "button:has-text('Log In')",
    "button:has-text('Login')",
    "button:has-text('Inloggen')",
    ".loginButton",
    "[data-label='Log In']",
]

# Login error message selectors
_ERROR_SELECTORS = [
    ".loginError",
    ".errorMsg",
    ".error-container",
    ".slds-form-error",
    "#error",
    "[id*='loginError']",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

@dataclass
class PortalCredentials:
    username: str
    password: str
    base_url: str = ""


def get_credentials(airflow_conn=None) -> PortalCredentials:
    """Load portal credentials from Airflow connection or environment variables."""
    _base_url = os.environ.get("SP1_BASE_URL", "")

    if airflow_conn is not None:
        username = airflow_conn.login or ""
        password = airflow_conn.password or ""
        extra = getattr(airflow_conn, "extra_dejson", {}) or {}
        base_url = extra.get("base_url", _base_url)
        if not username or not password:
            raise ValueError(
                "AIRFLOW_CONN_SOURCE_SP1 is missing login and/or password."
            )
        return PortalCredentials(username=username, password=password, base_url=base_url)

    username = os.environ.get("SP1_USERNAME", "")
    password = os.environ.get("SP1_PASSWORD", "")
    base_url = os.environ.get("SP1_BASE_URL", _base_url)

    if not username or not password:
        raise EnvironmentError(
            "Source SP1 portal credentials not found. "
            "Set AIRFLOW_CONN_SOURCE_SP1 or "
            "SP1_USERNAME + SP1_PASSWORD."
        )
    return PortalCredentials(username=username, password=password, base_url=base_url)


def login(page, creds: PortalCredentials) -> bool:
    """Authenticate the Playwright page against the Source SP1 portal.

    Steps:
      1. Navigate to creds.base_url.
      2. Detect if already authenticated (cookie reuse after state restore).
      3. Fill username + password and click submit.
      4. Wait for post-login DOM element.

    Returns:
        True if authentication appears successful; False on definitive failure.

    Raises:
        RuntimeError: if a login error message is detected.
    """
    log.info("Navigating to %s", creds.base_url)
    try:
        page.goto(creds.base_url, wait_until="domcontentloaded", timeout=60_000)
    except Exception as exc:
        raise RuntimeError(
            f"Could not reach {creds.base_url}: {exc}. "
            "Check network connectivity and portal availability."
        ) from exc

    if _is_logged_in(page):
        log.info("Session already active — skipping login.")
        return True

    # Locate username field (wait up to 10 s)
    username_sel = _find_visible_selector(page, _USERNAME_SELECTORS, timeout_ms=10_000)
    if username_sel is None:
        log.error(
            "Username field not found. Page URL: %s\nPage title: %s",
            page.url,
            page.title(),
        )
        return False

    page.fill(username_sel, creds.username)
    log.debug("Filled username (%s).", username_sel)

    password_sel = _find_visible_selector(page, _PASSWORD_SELECTORS, timeout_ms=5_000)
    if password_sel is None:
        log.error("Password field not found after username was found.")
        return False

    page.fill(password_sel, creds.password)
    log.debug("Filled password.")

    # Submit the form
    submit_sel = _find_visible_selector(page, _SUBMIT_SELECTORS, timeout_ms=3_000)
    if submit_sel:
        page.click(submit_sel)
        log.debug("Clicked submit (%s).", submit_sel)
    else:
        page.press(password_sel, "Enter")
        log.debug("Submitted via Enter key (no submit button found).")

    # Wait for navigation to settle
    try:
        page.wait_for_load_state("networkidle", timeout=_LOGIN_TIMEOUT_MS)
    except Exception:
        log.debug("Network-idle timeout during post-login wait — continuing.")

    # Check for error message before anything else
    error_msg = _get_login_error(page)
    if error_msg:
        raise RuntimeError(
            f"Login failed — portal returned error: {error_msg!r}. "
            "Verify credentials in AIRFLOW_CONN_SOURCE_SP1."
        )

    # Give JS-rendered content an extra moment to settle
    time.sleep(2)

    if _is_logged_in(page):
        log.info("Login successful. Current URL: %s", page.url)
        return True

    # Ambiguous state — could be MFA challenge or slow JS render
    log.warning(
        "Post-login state ambiguous. URL: %s — "
        "If MFA is enforced, see README_source_sp1.md § MFA fallback.",
        page.url,
    )
    # Return True to let extraction attempt proceed; it will fail cleanly if
    # the session is genuinely unauthenticated.
    return True


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _find_visible_selector(page, selectors: list[str], timeout_ms: int = 5_000) -> Optional[str]:
    """Return the first selector that matches a visible element, or None."""
    for sel in selectors:
        try:
            page.wait_for_selector(sel, state="visible", timeout=timeout_ms)
            return sel
        except Exception:
            pass
        # Shorter poll for subsequent selectors to avoid long cumulative waits
        timeout_ms = min(timeout_ms, 2_000)
    return None


def _is_logged_in(page) -> bool:
    """Heuristic: return True if the page looks like a logged-in SF Experience Cloud page."""
    # Fast check: login form absent AND we're past the /login path
    login_form = page.query_selector("input[name='username']")
    if login_form is not None:
        return False  # login form still visible — definitely not logged in

    if "login" in page.url.lower() and "/s/" not in page.url:
        return False

    # Look for any known post-login SF element
    for sel in _POST_LOGIN_SELECTORS:
        try:
            if page.query_selector(sel) is not None:
                return True
        except Exception:
            pass

    # If login form is gone and URL contains /s/, assume logged in
    if "/s/" in page.url and "login" not in page.url.lower():
        return True

    return False


def _get_login_error(page) -> Optional[str]:
    """Return visible login error text, or None."""
    for sel in _ERROR_SELECTORS:
        try:
            el = page.query_selector(sel)
            if el and el.is_visible():
                text = (el.text_content() or "").strip()
                if text:
                    return text
        except Exception:
            pass
    return None
