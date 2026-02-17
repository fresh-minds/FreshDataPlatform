#!/usr/bin/env python3
"""Send operational alerts to configured channels via webhook routing config."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

import requests
import yaml


def load_config(path: Path) -> dict[str, Any]:
    return yaml.safe_load(path.read_text(encoding="utf-8"))


def build_payload(channel: str, severity: str, summary: str, details: str) -> dict[str, Any]:
    if channel == "slack":
        return {
            "text": f"[{severity.upper()}] {summary}",
            "attachments": [{"text": details}],
        }
    if channel == "teams":
        return {
            "title": f"[{severity.upper()}] {summary}",
            "text": details,
        }
    if channel == "pagerduty":
        return {
            "routing_key": "integration-key-managed-in-webhook-receiver",
            "event_action": "trigger",
            "payload": {
                "summary": summary,
                "severity": severity,
                "source": "open-data-platform",
                "custom_details": {"details": details},
            },
        }
    raise ValueError(f"Unsupported channel: {channel}")


def route_alert(config: dict[str, Any], severity: str) -> list[dict[str, str]]:
    routes = []
    for route in config.get("routes", []):
        severities = [item.lower() for item in route.get("severities", [])]
        if severity.lower() in severities:
            routes.append(route)
    return routes


def send_alerts(config_path: Path, severity: str, summary: str, details: str) -> list[dict[str, Any]]:
    config = load_config(config_path)
    selected_routes = route_alert(config, severity)

    results = []
    for route in selected_routes:
        webhook_env = route["webhook_env"]
        webhook_url = os.getenv(webhook_env)

        if not webhook_url:
            results.append(
                {
                    "route": route["name"],
                    "channel": route["channel"],
                    "status": "skipped",
                    "reason": f"missing env {webhook_env}",
                }
            )
            continue

        payload = build_payload(route["channel"], severity, summary, details)
        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()

        results.append(
            {
                "route": route["name"],
                "channel": route["channel"],
                "status": "sent",
                "http_status": response.status_code,
            }
        )

    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Send alerts based on routing config")
    parser.add_argument("--severity", required=True, choices=["critical", "high", "medium", "low"])
    parser.add_argument("--summary", required=True)
    parser.add_argument("--details", default="")
    parser.add_argument("--config", default="ops/alerting_config.yaml")
    args = parser.parse_args()

    results = send_alerts(Path(args.config), args.severity, args.summary, args.details)
    print(json.dumps(results, indent=2))

    sent = [row for row in results if row["status"] == "sent"]
    if not sent:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
