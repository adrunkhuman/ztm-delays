# ruff: noqa: D103,S101
from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from collections.abc import Callable


def _load_classifier() -> Callable[[list[str]], list[Any]]:
    module_path = Path(__file__).with_name("audit_required.py")
    spec = importlib.util.spec_from_file_location("audit_required_under_test", module_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("failed to load audit_required module spec")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module.classify_changed_paths


classify_changed_paths = _load_classifier()


def test_unrelated_paths_do_not_require_audit() -> None:
    assert classify_changed_paths(["README.md", "poller/poller.py"]) == []


def test_orchestration_changes_require_audit() -> None:
    findings = classify_changed_paths(["airflow/dags/dag_daily_gps.py"])

    assert [finding.tier for finding in findings] == ["orchestration audit"]


def test_dbt_model_and_schema_changes_require_distinct_audits() -> None:
    findings = classify_changed_paths(
        [
            "dbt/models/intermediate/int_ping_trip.sql",
            "dbt/models/marts/schema.yml",
        ]
    )

    assert [finding.tier for finding in findings] == ["manual model audit", "manual test audit"]


def test_serving_and_docs_changes_require_expected_audits() -> None:
    findings = classify_changed_paths(["frontend/app.py", "docs/runbook.md"])

    assert [finding.tier for finding in findings] == ["serving export audit", "documentation review"]


def test_duplicate_matches_are_deduplicated() -> None:
    findings = classify_changed_paths(["dbt/tests/a.sql", "dbt/tests/b.sql"])

    assert [finding.tier for finding in findings] == ["manual test audit"]
