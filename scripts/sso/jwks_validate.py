#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from typing import Any

from scripts.sso.oidc import (
    OIDCFlowError,
    fetch_jwks,
    fetch_openid_configuration,
    normalize_keycloak_base_realm,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Fetch and validate Keycloak OIDC discovery + JWKS endpoints.")
    parser.add_argument("--keycloak-base-url", default=None)
    parser.add_argument("--keycloak-realm", default=None)
    parser.add_argument("--oidc-base-url", default=None)
    parser.add_argument("--timeout", type=float, default=20.0)
    return parser.parse_args()


def validate_jwks_structure(payload: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    keys = payload.get("keys")
    if not isinstance(keys, list) or not keys:
        errors.append("JWKS keys array is empty")
        return errors

    for index, key in enumerate(keys):
        if not isinstance(key, dict):
            errors.append(f"key[{index}] is not an object")
            continue
        for field in ("kid", "kty", "alg", "use"):
            if field not in key:
                errors.append(f"key[{index}] missing field '{field}'")
        if key.get("kty") == "RSA":
            if "n" not in key or "e" not in key:
                errors.append(f"RSA key[{index}] missing modulus/exponent")

    return errors


def main() -> int:
    args = parse_args()
    try:
        keycloak_base_url, keycloak_realm = normalize_keycloak_base_realm(
            args.keycloak_base_url,
            args.keycloak_realm,
            args.oidc_base_url,
        )
        metadata = fetch_openid_configuration(keycloak_base_url, keycloak_realm, timeout_s=args.timeout)
        jwks = fetch_jwks(metadata.jwks_uri, timeout_s=args.timeout)
        errors = validate_jwks_structure(jwks)
    except (ValueError, OIDCFlowError, Exception) as exc:  # noqa: BLE001
        print(f"[sso-jwks-validate] error: {exc}")
        return 1

    summary = {
        "issuer": metadata.issuer,
        "jwks_uri": metadata.jwks_uri,
        "key_count": len(jwks.get("keys", [])),
        "errors": errors,
    }
    print(json.dumps(summary, indent=2))

    if errors:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
