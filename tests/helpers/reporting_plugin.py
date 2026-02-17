from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest


@dataclass
class TestOutcome:
    nodeid: str
    outcome: str
    duration_s: float
    stage: str
    message: str | None


class QAReportingPlugin:
    def __init__(self, artifact_dir: Path) -> None:
        self.artifact_dir = artifact_dir
        self.started_at = datetime.now(timezone.utc)
        self._results: dict[str, TestOutcome] = {}

    @staticmethod
    def _extract_message(report: pytest.TestReport) -> str | None:
        if not report.failed:
            return None
        if not report.longrepr:
            return "unknown failure"

        if hasattr(report.longrepr, "reprcrash") and report.longrepr.reprcrash:
            return str(report.longrepr.reprcrash.message)
        return str(report.longrepr)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        if report.when == "call":
            self._results[report.nodeid] = TestOutcome(
                nodeid=report.nodeid,
                outcome=report.outcome,
                duration_s=report.duration,
                stage=report.when,
                message=self._extract_message(report),
            )
            return

        # Capture setup failures too, because they otherwise never reach call stage.
        if report.when == "setup" and report.failed:
            self._results[report.nodeid] = TestOutcome(
                nodeid=report.nodeid,
                outcome=report.outcome,
                duration_s=report.duration,
                stage=report.when,
                message=self._extract_message(report),
            )

    def pytest_sessionfinish(self, session: pytest.Session, exitstatus: int) -> None:
        self.artifact_dir.mkdir(parents=True, exist_ok=True)

        completed_at = datetime.now(timezone.utc)
        results = list(self._results.values())

        summary = {
            "started_at_utc": self.started_at.isoformat(),
            "completed_at_utc": completed_at.isoformat(),
            "exit_status": exitstatus,
            "total": len(results),
            "passed": len([r for r in results if r.outcome == "passed"]),
            "failed": len([r for r in results if r.outcome == "failed"]),
            "skipped": len([r for r in results if r.outcome == "skipped"]),
            "xfailed": len([r for r in results if r.outcome == "xfailed"]),
            "xpassed": len([r for r in results if r.outcome == "xpassed"]),
        }

        payload: dict[str, Any] = {
            "summary": summary,
            "tests": [asdict(result) for result in sorted(results, key=lambda item: item.nodeid)],
        }

        json_path = self.artifact_dir / "qa_report.json"
        json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

        lines: list[str] = []
        lines.append("# Data Platform QA Report")
        lines.append("")
        lines.append(f"- Started (UTC): `{summary['started_at_utc']}`")
        lines.append(f"- Completed (UTC): `{summary['completed_at_utc']}`")
        lines.append(f"- Exit status: `{summary['exit_status']}`")
        lines.append(f"- Total tests: `{summary['total']}`")
        lines.append(f"- Passed: `{summary['passed']}`")
        lines.append(f"- Failed: `{summary['failed']}`")
        lines.append(f"- Skipped: `{summary['skipped']}`")
        lines.append("")

        if summary["failed"] > 0:
            lines.append("## Failures")
            lines.append("")
            for result in sorted(results, key=lambda item: item.nodeid):
                if result.outcome != "failed":
                    continue
                lines.append(f"- `{result.nodeid}`")
                if result.message:
                    lines.append(f"  - {result.message}")
            lines.append("")

        lines.append("## Detailed Results")
        lines.append("")
        lines.append("| Test | Outcome | Stage | Duration (s) |")
        lines.append("| --- | --- | --- | ---: |")
        for result in sorted(results, key=lambda item: item.nodeid):
            lines.append(
                f"| `{result.nodeid}` | `{result.outcome}` | `{result.stage}` | {result.duration_s:.3f} |"
            )

        md_path = self.artifact_dir / "qa_report.md"
        md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")

        # Generate a simple self-contained HTML report to avoid hard dependency on pytest-html.
        html_lines: list[str] = []
        html_lines.append("<!doctype html>")
        html_lines.append("<html lang='en'>")
        html_lines.append("<head>")
        html_lines.append("<meta charset='utf-8' />")
        html_lines.append("<meta name='viewport' content='width=device-width, initial-scale=1' />")
        html_lines.append("<title>Data Platform QA Report</title>")
        html_lines.append("<style>")
        html_lines.append("body{font-family:Arial,sans-serif;margin:24px;color:#222;}")
        html_lines.append("h1,h2{margin:0 0 12px 0;}")
        html_lines.append("ul{margin-top:0;}")
        html_lines.append("table{border-collapse:collapse;width:100%;margin-top:12px;}")
        html_lines.append("th,td{border:1px solid #ddd;padding:8px;text-align:left;font-size:14px;}")
        html_lines.append("th{background:#f5f5f5;}")
        html_lines.append(".passed{color:#0a7b34;font-weight:700;}")
        html_lines.append(".failed{color:#b00020;font-weight:700;}")
        html_lines.append(".skipped{color:#6b7280;font-weight:700;}")
        html_lines.append("</style>")
        html_lines.append("</head>")
        html_lines.append("<body>")
        html_lines.append("<h1>Data Platform QA Report</h1>")
        html_lines.append("<ul>")
        html_lines.append(f"<li>Started (UTC): <code>{summary['started_at_utc']}</code></li>")
        html_lines.append(f"<li>Completed (UTC): <code>{summary['completed_at_utc']}</code></li>")
        html_lines.append(f"<li>Exit status: <code>{summary['exit_status']}</code></li>")
        html_lines.append(f"<li>Total tests: <code>{summary['total']}</code></li>")
        html_lines.append(
            f"<li>Passed: <span class='passed'>{summary['passed']}</span>, "
            f"Failed: <span class='failed'>{summary['failed']}</span>, "
            f"Skipped: <span class='skipped'>{summary['skipped']}</span></li>"
        )
        html_lines.append("</ul>")

        if summary["failed"] > 0:
            html_lines.append("<h2>Failures</h2>")
            html_lines.append("<ul>")
            for result in sorted(results, key=lambda item: item.nodeid):
                if result.outcome != "failed":
                    continue
                message = (result.message or "unknown failure").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                nodeid = result.nodeid.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                html_lines.append(f"<li><code>{nodeid}</code><br/>{message}</li>")
            html_lines.append("</ul>")

        html_lines.append("<h2>Detailed Results</h2>")
        html_lines.append("<table>")
        html_lines.append("<thead><tr><th>Test</th><th>Outcome</th><th>Stage</th><th>Duration (s)</th></tr></thead>")
        html_lines.append("<tbody>")
        for result in sorted(results, key=lambda item: item.nodeid):
            nodeid = result.nodeid.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            outcome = result.outcome.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            stage = result.stage.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
            html_lines.append(
                f"<tr><td><code>{nodeid}</code></td><td>{outcome}</td>"
                f"<td>{stage}</td><td>{result.duration_s:.3f}</td></tr>"
            )
        html_lines.append("</tbody></table>")
        html_lines.append("</body></html>")

        html_path = self.artifact_dir / "report.html"
        html_path.write_text("\n".join(html_lines) + "\n", encoding="utf-8")
