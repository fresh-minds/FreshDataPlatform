#!/usr/bin/env python3
from __future__ import annotations

import argparse
import datetime as dt
import os
import textwrap
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CaseResult:
    name: str
    classname: str
    status: str
    message: str


FLOW_MAPPING: dict[str, tuple[str, ...]] = {
    "smoke_config": (
        "test_openid_configuration_reachable_for_each_realm",
        "test_jwks_endpoint_reachable_and_keys_parseable",
        "test_client_redirect_uris_match_inventory",
        "test_auth_code_pkce_token_exchange_per_client",
    ),
    "browser_login": (
        "test_unauthenticated_access_redirects_to_keycloak",
        "test_login_establishes_session_for_each_app",
    ),
    "cross_app_sso": (
        "test_cross_app_sso_uses_existing_session",
    ),
    "logout_propagation": (
        "test_logout_propagation_matches_expected_design",
    ),
    "session_expiry": (
        "test_session_expiry_behavior",
    ),
    "api_authorization": (
        "test_access_token_claim_mapping",
        "test_keycloak_userinfo_authorization",
        "test_keycloak_admin_api_requires_elevated_role",
        "test_configured_protected_api_matrix",
    ),
    "negative_security": (
        "test_redirect_uri_injection_rejected",
        "test_invalid_code_verifier_rejected",
        "test_token_substitution_audience_protection",
        "test_clock_skew_handling_sanity",
        "test_refresh_token_rotation_behavior",
        "test_logout_capabilities_discovery",
    ),
}

REMEDIATION_HINTS: dict[str, str] = {
    "test_client_redirect_uris_match_inventory": (
        "Update Keycloak client config to align redirect URIs/origins with deployed app URLs; "
        "remove wildcard origins and explicitly list trusted origins."
    ),
    "test_auth_code_pkce_token_exchange_per_client": (
        "Verify client credentials and redirect URIs; ensure app is configured for auth code + PKCE "
        "and token endpoint accepts the configured client authentication method."
    ),
    "test_unauthenticated_access_redirects_to_keycloak": (
        "Check app auth middleware and OIDC client settings; ensure unauthenticated routes force IdP redirect "
        "instead of local fallback login paths."
    ),
    "test_cross_app_sso_uses_existing_session": (
        "Ensure all apps trust the same Keycloak realm/session cookie domain and do not force prompt=login "
        "or isolate auth context by origin policy."
    ),
    "test_logout_propagation_matches_expected_design": (
        "Configure front/back-channel logout consistently and map app logout endpoints to Keycloak end-session "
        "to prevent stale sessions across apps."
    ),
    "test_keycloak_admin_api_requires_elevated_role": (
        "Confirm admin role mapping and token audience for admin endpoints; non-admin tokens should stay 401/403."
    ),
    "test_redirect_uri_injection_rejected": (
        "Restrict clients to allowlisted redirect URIs and verify no wildcard redirect patterns are accepted."
    ),
    "test_invalid_code_verifier_rejected": (
        "Enforce PKCE verifier checks on token exchange and disable non-PKCE fallbacks for public clients."
    ),
    "test_token_substitution_audience_protection": (
        "Validate audience and azp checks on every API/gateway so tokens from other clients are rejected."
    ),
    "test_refresh_token_rotation_behavior": (
        "Enable refresh token rotation + reuse detection in realm/client settings if required by policy."
    ),
}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate SSO markdown report from pytest JUnit XML")
    parser.add_argument("--junit-xml", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--artifact-dir", required=True)
    parser.add_argument("--inventory", default="sso-test-inventory.md")
    parser.add_argument("--environment", default=os.getenv("SSO_ENVIRONMENT", "dev"))
    return parser.parse_args()


def parse_junit(path: Path) -> list[CaseResult]:
    tree = ET.parse(path)
    root = tree.getroot()

    cases: list[CaseResult] = []
    for testcase in root.findall(".//testcase"):
        name = testcase.attrib.get("name", "")
        classname = testcase.attrib.get("classname", "")

        status = "passed"
        message = ""

        failure = testcase.find("failure")
        error = testcase.find("error")
        skipped = testcase.find("skipped")

        if failure is not None:
            status = "failed"
            message = (failure.attrib.get("message") or (failure.text or "")).strip()
        elif error is not None:
            status = "failed"
            message = (error.attrib.get("message") or (error.text or "")).strip()
        elif skipped is not None:
            status = "skipped"
            message = (skipped.attrib.get("message") or (skipped.text or "")).strip()

        cases.append(CaseResult(name=name, classname=classname, status=status, message=message))

    return cases


def summarize_flow(cases: list[CaseResult], test_names: tuple[str, ...]) -> str:
    matched = [case for case in cases if any(name in case.name for name in test_names)]
    if not matched:
        return "not_run"
    if any(case.status == "failed" for case in matched):
        return "fail"
    if all(case.status == "skipped" for case in matched):
        return "skipped"
    if any(case.status == "passed" for case in matched):
        return "pass"
    return "not_run"


def flow_emoji(status: str) -> str:
    if status == "pass":
        return "PASS"
    if status == "fail":
        return "FAIL"
    if status == "skipped":
        return "SKIP"
    return "NOT RUN"


def app_flow_table(cases: list[CaseResult]) -> str:
    apps = ["airflow", "datahub", "minio"]
    flow_statuses = {flow: summarize_flow(cases, tests) for flow, tests in FLOW_MAPPING.items()}

    lines = [
        "| App | Smoke | Browser Login | Cross-App SSO | Logout | Session Expiry | API AuthZ | Negative Security |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for app in apps:
        lines.append(
            "| "
            f"{app} | {flow_emoji(flow_statuses['smoke_config'])} | {flow_emoji(flow_statuses['browser_login'])} "
            f"| {flow_emoji(flow_statuses['cross_app_sso'])} | {flow_emoji(flow_statuses['logout_propagation'])} "
            f"| {flow_emoji(flow_statuses['session_expiry'])} | {flow_emoji(flow_statuses['api_authorization'])} "
            f"| {flow_emoji(flow_statuses['negative_security'])} |"
        )
    return "\n".join(lines)


def compute_overall_status(cases: list[CaseResult]) -> str:
    if any(case.status == "failed" for case in cases):
        return "FAIL"

    critical_tests = (
        "test_unauthenticated_access_redirects_to_keycloak",
        "test_login_establishes_session_for_each_app",
        "test_cross_app_sso_uses_existing_session",
        "test_logout_propagation_matches_expected_design",
        "test_keycloak_userinfo_authorization",
        "test_redirect_uri_injection_rejected",
    )

    for test_name in critical_tests:
        matches = [case for case in cases if test_name in case.name]
        if not matches:
            return "FAIL"
        if any(case.status in {"skipped"} for case in matches):
            return "FAIL"

    return "PASS"


def failed_findings(cases: list[CaseResult]) -> list[str]:
    findings: list[str] = []
    for case in cases:
        if case.status != "failed":
            continue

        hint = ""
        for key, remediation in REMEDIATION_HINTS.items():
            if key in case.name:
                hint = remediation
                break

        message = case.message.replace("\n", " ").strip()
        if len(message) > 280:
            message = message[:280] + "..."

        finding = textwrap.dedent(
            f"""
            - Test: `{case.classname}::{case.name}`
            - Evidence: {message or 'See junit XML + trace/screenshot artifacts.'}
            - Likely root cause: {hint or 'See assertion details and request/response artifacts.'}
            - Remediation: {hint or 'Apply targeted fix based on failing assertion, then rerun sso-e2e.'}
            """
        ).strip()
        findings.append(finding)

    return findings


def main() -> int:
    args = parse_args()
    junit_path = Path(args.junit_xml).resolve()
    output_path = Path(args.output).resolve()
    artifact_dir = Path(args.artifact_dir).resolve()
    inventory_path = Path(args.inventory).resolve()

    if not junit_path.exists():
        raise FileNotFoundError(f"JUnit file not found: {junit_path}")

    cases = parse_junit(junit_path)
    total = len(cases)
    passed = sum(case.status == "passed" for case in cases)
    failed = sum(case.status == "failed" for case in cases)
    skipped = sum(case.status == "skipped" for case in cases)

    overall = compute_overall_status(cases)
    findings = failed_findings(cases)

    report = [
        "# SSO E2E Test Report",
        "",
        f"- Generated at: {dt.datetime.now(dt.timezone.utc).isoformat()}",
        f"- Environment: `{args.environment}`",
        f"- Overall result: **{overall}**",
        f"- Inventory source: `{inventory_path}`",
        "",
        "## Summary",
        "",
        f"- Total tests: {total}",
        f"- Passed: {passed}",
        f"- Failed: {failed}",
        f"- Skipped: {skipped}",
        "",
        "## Apps x Flows",
        "",
        app_flow_table(cases),
        "",
        "## Evidence",
        "",
        f"- JUnit XML: `{junit_path}`",
        f"- Artifacts root: `{artifact_dir}`",
        f"- HAR files: `{artifact_dir / 'har'}`",
        f"- Trace files: `{artifact_dir / 'traces'}`",
        f"- Screenshots: `{artifact_dir / 'screenshots'}`",
        f"- Logs: `{artifact_dir / 'logs'}`",
        "",
        "## Failures and Fixes",
        "",
    ]

    if findings:
        report.extend(findings)
    else:
        report.append("- No failing tests were observed in this run.")

    report.extend(
        [
            "",
            "## Repro Steps",
            "",
            "1. Set SSO environment variables in `.env` or shell.",
            "2. Start the target stack (local/dev/stage) with Keycloak + integrated apps.",
            "3. Run `make test-sso`.",
            "4. Inspect `tests/sso/artifacts/latest` and this report.",
        ]
    )

    output_path.write_text("\n".join(report) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
