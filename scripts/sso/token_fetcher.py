#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

from scripts.sso.oidc import (
    OIDCFlowError,
    fetch_tokens_auth_code_pkce,
    normalize_keycloak_base_realm,
    redact_secret,
)


def _redact_payload(payload: dict[str, Any]) -> dict[str, Any]:
    redacted: dict[str, Any] = {}
    for key, value in payload.items():
        if isinstance(value, dict):
            redacted[key] = _redact_payload(value)
            continue
        if key in {
            "access_token",
            "id_token",
            "refresh_token",
            "code_verifier",
            "state",
            "nonce",
        }:
            redacted[key] = redact_secret(str(value), keep=8)
        else:
            redacted[key] = value
    return redacted


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Fetch OIDC tokens via Authorization Code + PKCE using Keycloak login form automation.",
    )
    parser.add_argument("--keycloak-base-url", default=None)
    parser.add_argument("--keycloak-realm", default=None)
    parser.add_argument("--oidc-base-url", default=None)
    parser.add_argument("--client-id", required=True)
    parser.add_argument("--client-secret", default=None)
    parser.add_argument("--redirect-uri", required=True)
    parser.add_argument("--username", required=True)
    parser.add_argument("--password", required=True)
    parser.add_argument("--scope", default="openid profile email")
    parser.add_argument("--otp-code", default=None)
    parser.add_argument("--timeout", type=float, default=20.0)
    parser.add_argument("--output", default=None, help="Write full token response JSON to this path")
    parser.add_argument(
        "--show-secrets",
        action="store_true",
        help="Print raw tokens to stdout (default stdout is redacted)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    try:
        keycloak_base_url, keycloak_realm = normalize_keycloak_base_realm(
            args.keycloak_base_url,
            args.keycloak_realm,
            args.oidc_base_url,
        )

        tokens = fetch_tokens_auth_code_pkce(
            keycloak_base_url=keycloak_base_url,
            realm=keycloak_realm,
            client_id=args.client_id,
            client_secret=args.client_secret,
            redirect_uri=args.redirect_uri,
            username=args.username,
            password=args.password,
            scope=args.scope,
            timeout_s=args.timeout,
            otp_code=args.otp_code,
        )
    except (ValueError, OIDCFlowError) as exc:
        print(f"[sso-token-fetcher] error: {exc}")
        return 1

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(tokens, indent=2), encoding="utf-8")

    payload_for_stdout = tokens if args.show_secrets else _redact_payload(tokens)
    print(json.dumps(payload_for_stdout, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
