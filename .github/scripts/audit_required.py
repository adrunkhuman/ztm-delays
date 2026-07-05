from __future__ import annotations

import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AuditRule:
    """Changed-path rule that maps a file pattern to an audit tier."""

    pattern: re.Pattern[str]
    tier: str
    reason: str


RULES = (
    AuditRule(re.compile(r"^dbt/macros/"), "manual dbt audit", "dbt macro changes can alter compiled SQL broadly."),
    AuditRule(
        re.compile(r"^dbt/models/(intermediate|marts)/.*\.sql$"),
        "manual model audit",
        "intermediate or mart SQL changed.",
    ),
    AuditRule(
        re.compile(r"^dbt/models/.*/schema\.yml$|^dbt/tests/"),
        "manual test audit",
        "dbt test, grain, key, or contract metadata changed.",
    ),
    AuditRule(
        re.compile(r"^airflow/dags/dag_(daily_gps|gtfs_load)\.py$|^airflow/dags/ztm_airflow_common\.py$"),
        "orchestration audit",
        "Airflow dbt cadence, selector, or shared command code changed.",
    ),
    AuditRule(
        re.compile(r"^airflow/dags/dag_serving_export\.py$|^docs/serving_contract\.md$|^frontend/"),
        "serving export audit",
        "serving export or frontend contract path changed.",
    ),
    AuditRule(
        re.compile(r"^docs/runbook\.md$|^docs/warehouse\.md$|^dbt/README\.md$|^airflow/README\.md$"),
        "documentation review",
        "operational runbook or warehouse contract documentation changed.",
    ),
)


@dataclass(frozen=True)
class AuditFinding:
    """Audit tier reported for at least one changed path."""

    tier: str
    reason: str


def classify_changed_paths(paths: list[str]) -> list[AuditFinding]:
    """Return deduplicated audit findings for changed repository paths."""
    findings: list[AuditFinding] = []
    seen: set[AuditFinding] = set()

    for path in paths:
        normalized_path = path.replace("\\", "/")
        for rule in RULES:
            if not rule.pattern.search(normalized_path):
                continue
            finding = AuditFinding(rule.tier, rule.reason)
            if finding not in seen:
                findings.append(finding)
                seen.add(finding)

    return findings


def render_summary(findings: list[AuditFinding]) -> str:
    """Render the GitHub Actions step summary for audit findings."""
    lines = [
        "## Audit Tier Report",
        "",
        "This workflow is advisory only. It does not run billable BigQuery/dbt audits.",
        "",
    ]
    if not findings:
        lines.append("No audit-tier trigger paths changed.")
    else:
        lines.extend(f"- {finding.tier}: {finding.reason}" for finding in findings)
    return "\n".join(lines) + "\n"


def main() -> int:
    """Read changed paths from stdin and write advisory audit output."""
    paths = [line.strip() for line in sys.stdin if line.strip()]
    findings = classify_changed_paths(paths)
    summary = render_summary(findings)

    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with Path(summary_path).open("a", encoding="utf-8") as summary_file:
            summary_file.write(summary)
    else:
        sys.stdout.write(summary)

    if findings:
        print(
            "::warning title=Manual audit required::Changed paths require explicit audit selection before billable execution."
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
