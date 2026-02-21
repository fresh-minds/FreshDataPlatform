from __future__ import annotations

import importlib.util
import sys
import time
from pathlib import Path

import pytest


@pytest.fixture()
def minio_bridge_module(monkeypatch):
    monkeypatch.setenv("KEYCLOAK_MINIO_CLIENT_SECRET", "secret")

    module_path = Path(__file__).resolve().parents[2] / "ops" / "minio-sso-bridge" / "app.py"
    spec = importlib.util.spec_from_file_location("minio_sso_bridge_app", module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    try:
        spec.loader.exec_module(module)
    except ModuleNotFoundError as exc:
        pytest.skip(f"MinIO SSO bridge dependencies unavailable: {exc.name}")
    return module


def test_state_cookie_roundtrip(minio_bridge_module) -> None:
    expected_issued_at = int(time.time())
    cookie = minio_bridge_module._make_state_cookie_value("state-1", "nonce-1", expected_issued_at)

    state, nonce, parsed_issued_at = minio_bridge_module._parse_state_cookie(cookie)

    assert state == "state-1"
    assert nonce == "nonce-1"
    assert parsed_issued_at == expected_issued_at


def test_state_cookie_tamper_detected(minio_bridge_module) -> None:
    cookie = minio_bridge_module._make_state_cookie_value("state-1", "nonce-1", int(time.time()))
    tampered = cookie.replace("state-1", "state-2")

    with pytest.raises(ValueError, match="state signature mismatch"):
        minio_bridge_module._parse_state_cookie(tampered)


def test_extract_sts_error_message_reads_xml_message(minio_bridge_module) -> None:
    xml = """
    <ErrorResponse xmlns=\"https://sts.amazonaws.com/doc/2011-06-15/\">
      <Error>
        <Type>Sender</Type>
        <Code>InvalidIdentityToken</Code>
        <Message>Token invalid</Message>
      </Error>
    </ErrorResponse>
    """

    message = minio_bridge_module._extract_sts_error_message(xml)

    assert message == "Token invalid"
