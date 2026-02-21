from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

fastapi = pytest.importorskip("fastapi")
HTTPException = fastapi.HTTPException


@pytest.fixture()
def portal_api_module(monkeypatch):
    monkeypatch.setenv("KEYCLOAK_ADMIN_USER", "admin")
    monkeypatch.setenv("KEYCLOAK_ADMIN_PASSWORD", "admin-pass")

    module_path = Path(__file__).resolve().parents[2] / "ops" / "portal-api" / "app.py"
    spec = importlib.util.spec_from_file_location("portal_api_app", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        pytest.skip(f"Portal API dependencies unavailable: {exc.name}")
    return module


def test_require_admin_allows_admin_role(portal_api_module) -> None:
    portal_api_module._require_admin({"realm_access": {"roles": ["admin", "user"]}})


def test_require_admin_rejects_non_admin(portal_api_module) -> None:
    with pytest.raises(HTTPException) as exc:
        portal_api_module._require_admin({"realm_access": {"roles": ["viewer"]}})

    assert exc.value.status_code == 403


def test_list_users_requires_admin_role(monkeypatch, portal_api_module) -> None:
    monkeypatch.setattr(portal_api_module, "_extract_bearer_token", lambda request: "token")
    monkeypatch.setattr(
        portal_api_module,
        "_verify_portal_token",
        lambda token: {"sub": "u-1", "realm_access": {"roles": ["viewer"]}},
    )

    with pytest.raises(HTTPException) as exc:
        portal_api_module.list_users(request=None)

    assert exc.value.status_code == 403


def test_list_users_returns_users_for_admin(monkeypatch, portal_api_module) -> None:
    monkeypatch.setattr(portal_api_module, "_extract_bearer_token", lambda request: "token")
    monkeypatch.setattr(
        portal_api_module,
        "_verify_portal_token",
        lambda token: {
            "sub": "u-admin",
            "preferred_username": "admin",
            "realm_access": {"roles": ["admin"]},
        },
    )
    monkeypatch.setattr(portal_api_module, "_keycloak_admin_token", lambda: "kc-admin-token")
    monkeypatch.setattr(
        portal_api_module,
        "_fetch_users",
        lambda token: [{"id": "1", "username": "alice"}, {"id": "2", "username": "bob"}],
    )

    result = portal_api_module.list_users(request=None)

    assert result["total"] == 2
    assert len(result["users"]) == 2
