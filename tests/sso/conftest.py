from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Generator

import httpx
import pytest

from scripts.sso.oidc import fetch_openid_configuration
from tests.sso.settings import SSOApp, SSOSettings, load_sso_settings


@dataclass(frozen=True)
class SSOArtifacts:
    run_id: str
    run_dir: Path
    latest_dir: Path
    screenshots_dir: Path
    traces_dir: Path
    har_dir: Path
    logs_dir: Path


def _sanitize_filename(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_.-]+", "_", value)


def _prepare_artifacts(settings: SSOSettings) -> SSOArtifacts:
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = settings.artifact_root / run_id
    latest_dir = settings.artifact_root / "latest"

    screenshots_dir = run_dir / "screenshots"
    traces_dir = run_dir / "traces"
    har_dir = run_dir / "har"
    logs_dir = run_dir / "logs"

    for path in (screenshots_dir, traces_dir, har_dir, logs_dir):
        path.mkdir(parents=True, exist_ok=True)

    manifest = {
        "run_id": run_id,
        "started_at_utc": datetime.now(timezone.utc).isoformat(),
        "artifact_root": str(settings.artifact_root),
        "keycloak_base_url": settings.keycloak_base_url,
        "realms": list(settings.keycloak_realms),
        "apps": [app.name for app in settings.apps],
    }
    (run_dir / "run_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    if latest_dir.exists() or latest_dir.is_symlink():
        if latest_dir.is_symlink() or latest_dir.is_file():
            latest_dir.unlink()
        else:
            for child in latest_dir.iterdir():
                if child.is_file() or child.is_symlink():
                    child.unlink()
                elif child.is_dir():
                    for nested in child.rglob("*"):
                        if nested.is_file() or nested.is_symlink():
                            nested.unlink()
                    for nested in sorted(child.rglob("*"), reverse=True):
                        if nested.is_dir():
                            nested.rmdir()
                    child.rmdir()
            latest_dir.rmdir()

    latest_dir.parent.mkdir(parents=True, exist_ok=True)
    latest_dir.symlink_to(run_dir, target_is_directory=True)

    return SSOArtifacts(
        run_id=run_id,
        run_dir=run_dir,
        latest_dir=latest_dir,
        screenshots_dir=screenshots_dir,
        traces_dir=traces_dir,
        har_dir=har_dir,
        logs_dir=logs_dir,
    )


def pytest_generate_tests(metafunc: pytest.Metafunc) -> None:
    if "browser_name" in metafunc.fixturenames:
        settings = load_sso_settings()
        metafunc.parametrize("browser_name", settings.browser_names, scope="session")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item: pytest.Item, call: pytest.CallInfo[None]) -> Generator[None, None, None]:
    outcome = yield
    report = outcome.get_result()
    setattr(item, f"rep_{report.when}", report)


@pytest.fixture(scope="session")
def sso_settings() -> SSOSettings:
    return load_sso_settings()


@pytest.fixture(scope="session")
def sso_artifacts(sso_settings: SSOSettings) -> SSOArtifacts:
    return _prepare_artifacts(sso_settings)


@pytest.fixture(scope="session")
def keycloak_metadata(sso_settings: SSOSettings) -> dict[str, object]:
    metadata = fetch_openid_configuration(
        sso_settings.keycloak_base_url,
        sso_settings.keycloak_primary_realm,
    )
    return {
        "issuer": metadata.issuer,
        "authorization_endpoint": metadata.authorization_endpoint,
        "token_endpoint": metadata.token_endpoint,
        "jwks_uri": metadata.jwks_uri,
        "userinfo_endpoint": metadata.userinfo_endpoint,
        "end_session_endpoint": metadata.end_session_endpoint,
    }


@pytest.fixture(scope="session")
def app_map(sso_settings: SSOSettings) -> dict[str, SSOApp]:
    return {app.name: app for app in sso_settings.apps}


@pytest.fixture(scope="session")
def playwright_driver() -> Generator[object, None, None]:
    playwright_sync = pytest.importorskip("playwright.sync_api")
    with playwright_sync.sync_playwright() as playwright:
        yield playwright


@pytest.fixture(scope="session")
def browser(playwright_driver: object, browser_name: str) -> Generator[object, None, None]:
    browser_type = getattr(playwright_driver, browser_name)
    browser = browser_type.launch(
        headless=True,
        args=[
            "--host-resolver-rules=MAP keycloak 127.0.0.1",
        ],
    )
    yield browser
    browser.close()


@pytest.fixture
def browser_context_factory(
    browser: object,
    sso_artifacts: SSOArtifacts,
    request: pytest.FixtureRequest,
) -> Generator[Callable[[str], object], None, None]:
    contexts: list[tuple[object, str]] = []

    def _create(label: str) -> object:
        node_id = _sanitize_filename(request.node.nodeid)
        context_name = _sanitize_filename(label)
        har_path = sso_artifacts.har_dir / f"{node_id}-{context_name}.har"

        context = browser.new_context(
            record_har_path=str(har_path),
            ignore_https_errors=True,
        )
        context.tracing.start(screenshots=True, snapshots=True, sources=True)
        contexts.append((context, context_name))
        return context

    yield _create

    node_id = _sanitize_filename(request.node.nodeid)
    test_failed = bool(getattr(request.node, "rep_call", None) and request.node.rep_call.failed)

    for context, context_name in contexts:
        if test_failed:
            for page_index, page in enumerate(context.pages):
                screenshot_path = sso_artifacts.screenshots_dir / f"{node_id}-{context_name}-{page_index}.png"
                try:
                    page.screenshot(path=str(screenshot_path), full_page=True)
                except Exception:  # noqa: BLE001
                    pass

        trace_path = sso_artifacts.traces_dir / f"{node_id}-{context_name}.zip"
        try:
            context.tracing.stop(path=str(trace_path))
        finally:
            context.close()


@pytest.fixture
def api_client() -> Generator[httpx.Client, None, None]:
    with httpx.Client(timeout=20.0, follow_redirects=False) as client:
        yield client


@pytest.fixture
def write_log(sso_artifacts: SSOArtifacts) -> Callable[[str, str], None]:
    def _write(name: str, content: str) -> None:
        target = sso_artifacts.logs_dir / f"{_sanitize_filename(name)}.log"
        target.write_text(content, encoding="utf-8")

    return _write
