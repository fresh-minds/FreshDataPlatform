#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path

from scripts.sso.oidc import decode_jwt_claims, token_is_expired, token_not_yet_valid


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Assert JWT claim/role/group expectations.")
    parser.add_argument("--token", default=None)
    parser.add_argument("--token-file", default=None)
    parser.add_argument("--expect-claim", action="append", default=[], help="Format: key=value")
    parser.add_argument("--expect-role", action="append", default=[], help="Realm role expected in realm_access.roles")
    parser.add_argument(
        "--expect-resource-role",
        action="append",
        default=[],
        help="Format: client_id:role expected in resource_access[client_id].roles",
    )
    parser.add_argument("--expect-group", action="append", default=[])
    parser.add_argument("--leeway-seconds", type=int, default=60)
    parser.add_argument("--print-claims", action="store_true")
    return parser.parse_args()


def load_token(args: argparse.Namespace) -> str:
    if args.token:
        return args.token.strip()
    if args.token_file:
        return Path(args.token_file).read_text(encoding="utf-8").strip()
    raise ValueError("Provide either --token or --token-file")


def parse_key_value(item: str) -> tuple[str, str]:
    if "=" not in item:
        raise ValueError(f"Invalid --expect-claim value '{item}'; expected key=value")
    key, value = item.split("=", 1)
    return key.strip(), value.strip()


def parse_client_role(item: str) -> tuple[str, str]:
    if ":" not in item:
        raise ValueError(f"Invalid --expect-resource-role value '{item}'; expected client:role")
    client_id, role = item.split(":", 1)
    return client_id.strip(), role.strip()


def main() -> int:
    args = parse_args()

    try:
        token = load_token(args)
        claims = decode_jwt_claims(token)
    except Exception as exc:  # noqa: BLE001
        print(f"[sso-role-assert] error: {exc}")
        return 1

    failures: list[str] = []

    if token_not_yet_valid(claims, leeway_seconds=args.leeway_seconds):
        failures.append("Token is not yet valid")
    if token_is_expired(claims, leeway_seconds=args.leeway_seconds):
        failures.append("Token is expired")

    for item in args.expect_claim:
        try:
            key, expected_value = parse_key_value(item)
        except ValueError as exc:
            print(f"[sso-role-assert] error: {exc}")
            return 2

        actual_value = claims.get(key)
        if str(actual_value) != expected_value:
            failures.append(f"Claim mismatch for '{key}': expected '{expected_value}', got '{actual_value}'")

    realm_roles = claims.get("realm_access", {}).get("roles", [])
    if not isinstance(realm_roles, list):
        realm_roles = []
    for expected_role in args.expect_role:
        if expected_role not in realm_roles:
            failures.append(f"Missing realm role '{expected_role}'")

    resource_access = claims.get("resource_access", {})
    if not isinstance(resource_access, dict):
        resource_access = {}

    for item in args.expect_resource_role:
        try:
            client_id, role = parse_client_role(item)
        except ValueError as exc:
            print(f"[sso-role-assert] error: {exc}")
            return 2

        client_roles = resource_access.get(client_id, {}).get("roles", [])
        if not isinstance(client_roles, list) or role not in client_roles:
            failures.append(f"Missing client role '{role}' for resource '{client_id}'")

    groups = claims.get("groups", [])
    if not isinstance(groups, list):
        groups = []
    for expected_group in args.expect_group:
        if expected_group not in groups:
            failures.append(f"Missing group '{expected_group}'")

    result = {
        "subject": claims.get("sub"),
        "issuer": claims.get("iss"),
        "audience": claims.get("aud"),
        "realm_roles": realm_roles,
        "groups": groups,
        "failures": failures,
    }

    if args.print_claims:
        result["claims"] = claims

    print(json.dumps(result, indent=2))
    return 0 if not failures else 3


if __name__ == "__main__":
    raise SystemExit(main())
