from __future__ import annotations

import json
from typing import TYPE_CHECKING

import duckdb
import pytest

from ztm_frontend import queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


def _feed() -> dict[str, object]:
    return {
        "version": 1,
        "hour_start": "2026-10-05T10:00:00Z",
        "hour_end": "2026-10-05T11:00:00Z",
        "evaluated_at": "2026-10-05T11:25:00Z",
        "vehicle_types": {
            "bus": {
                "status": "degraded",
                "monitored_minutes": 60,
                "baseline_samples": 3,
                "accepted_rows": 20,
                "parsed_rows": 100,
            },
            "tram": {
                "status": "warming_up",
                "monitored_minutes": 30,
                "baseline_samples": 0,
                "accepted_rows": None,
                "parsed_rows": None,
            },
        },
        "recent_intervals": [
            {"mode": "bus", "start_at": "2026-10-05T10:15:00Z", "end_at": "2026-10-05T11:00:00Z", "reason": "low_fleet"}
        ],
    }


def test_status_renders_feed_confidence_and_intervals(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: False)
    monkeypatch.setattr(
        queries,
        "get_status",
        lambda _: {"metadata": {"poller_status": {"feed_health": _feed()}}, "status_summary": {}, "status_days": []},
    )
    html = create_app().test_client().get("/status").get_data(as_text=True)
    assert "GPS feed health" in html
    assert "not live monitoring" in html
    assert "degraded" in html
    assert "warming up" in html
    assert "20 / 100" in html
    assert "n/a / n/a" in html
    assert "low fleet" in html
    assert "2026-10-05 10:15:00 UTC" in html
    assert "not service cancellations" in html


@pytest.mark.parametrize("feed", [None, {}, {"status": "unknown", "vehicle_types": {}}])
def test_old_export_and_unknown_metrics_do_not_render_zero(monkeypatch: pytest.MonkeyPatch, feed: object) -> None:
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: False)
    monkeypatch.setattr(
        queries,
        "get_status",
        lambda _: {"metadata": {"poller_status": {"feed_health": feed}}, "status_summary": {}, "status_days": []},
    )
    html = create_app().test_client().get("/status").get_data(as_text=True)
    assert "unknown" in html if isinstance(feed, dict) and feed.get("status") else "not monitored" in html
    assert "n/a / n/a" in html
    assert "0 / 0" not in html


def test_sidecar_feed_health_requires_matching_export_id_and_filters_extra_fields(tmp_path: Path) -> None:
    path = tmp_path / "feed.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute("""
            create table export_metadata as select 'current' as export_id, 'alpha-1' as export_version,
            'current_pipeline_provisional' as source_mode, timestamp '2026-10-05 12:00:00' as exported_at,
            1::ubigint as source_row_count, 10::ubigint as duckdb_file_size_bytes
        """)
    feed = _feed()
    feed["private_field"] = "private-host"
    sidecar = {"export_id": "old", "poller_status": {"status": "ok", "feed_health": feed}}
    metadata_path = path.with_suffix(".duckdb.meta.json")
    metadata_path.write_text(json.dumps(sidecar), encoding="utf-8")
    assert "poller_status" not in queries.get_export_metadata(path)
    sidecar["export_id"] = "current"
    metadata_path.write_text(json.dumps(sidecar), encoding="utf-8")
    result = queries.get_export_metadata(path)["poller_status"]["feed_health"]
    assert result["vehicle_types"]["bus"]["status"] == "degraded"
    assert result["recent_intervals"] == feed["recent_intervals"]
    assert "private-host" not in json.dumps(result)


def test_stale_snapshot_shows_the_state_it_was_evaluated_in(monkeypatch: pytest.MonkeyPatch) -> None:
    feed = _feed()
    feed["vehicle_types"]["bus"].update(status="stale", status_at_evaluation="degraded")  # type: ignore[index]
    monkeypatch.setattr(queries, "get_export_metadata", lambda _: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _: False)
    monkeypatch.setattr(
        queries,
        "get_status",
        lambda _: {"metadata": {"poller_status": {"feed_health": feed}}, "status_summary": {}, "status_days": []},
    )
    html = create_app().test_client().get("/status").get_data(as_text=True)
    assert "was degraded" in html
