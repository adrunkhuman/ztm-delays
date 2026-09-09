from __future__ import annotations

import re
from http import HTTPStatus
from typing import TYPE_CHECKING

import duckdb

from ztm_frontend import queries
from ztm_frontend.app import create_app
from ztm_frontend.queries import get_status

if TYPE_CHECKING:
    from pathlib import Path

    import pytest


def test_status_page_renders_current_pipeline_contract(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    recent_day_count = 8
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table export_metadata as select
                'export-1' as export_id,
                'alpha-1' as export_version,
                'current_pipeline_provisional' as source_mode,
                timestamp '2026-07-14 03:12:00' as exported_at,
                1::ubigint as source_row_count,
                100::ubigint as duckdb_file_size_bytes;

            create table mart_pipeline_status as select
                date '2026-07-13' - range::integer as service_date,
                'bus' as mode,
                1.0 as service_coverage_ratio,
                1.0 as completeness_ratio,
                10 as trips_complete,
                2 as trips_partial,
                1 as trips_broken
            from range(9);

            create table mart_pipeline_status_recent_summary as select
                'bus' as mode,
                1 as day_count,
                date '2026-07-13' as first_date,
                date '2026-07-13' as last_date,
                1.0 as completeness_ratio,
                1.0 as service_coverage_ratio,
                10 as trips_complete,
                1 as trips_broken,
                600 as expected_service_minutes,
                580 as observed_service_minutes,
                1.0 as health_ratio,
                'good' as health_label;
            """
        )
    monkeypatch.setenv("ZTM_DUCKDB_PATH", str(db_path))

    status = get_status(db_path)

    response = create_app().test_client().get("/status")

    assert len(status["pipeline_status"]["bus"]) == recent_day_count
    assert str(status["pipeline_status"]["bus"][-1]["service_date"]) == "2026-07-06"
    assert response.status_code == HTTPStatus.OK
    assert b"observed minutes" not in response.data
    assert b"service-minute coverage" in response.data
    assert b"Trip coverage &amp; quality counts" in response.data
    assert b'<th colspan="4" scope="colgroup">Bus</th>' in response.data
    assert b'<th scope="col">Clean</th>' in response.data
    assert b'<th scope="col">Partial</th>' in response.data
    assert b'<th scope="col">Broken</th>' in response.data
    assert b"/?date=2026-07-13&amp;window=day" in response.data
    assert b"2026-07-14 03:12:00 UTC" in response.data
    assert b"poller snapshot" in response.data
    assert b"no recent data" in response.data
    expected_partial = 2
    assert status["status_summary"]["bus"]["trips_partial"] == expected_partial
    assert b"matched" not in response.data


def test_stop_page_preserves_independent_picker_page(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_stops(*args: object) -> dict[str, object]:
        captured["page"] = args[-3]
        captured["picker_page"] = args[-2]
        captured["window"] = args[-1]
        captured["date"] = args[5]
        return {
            "selected_stop_group_id": None,
            "selected_mode": "bus",
            "selected_rank": "worst",
            "selected_date": "2026-06-30",
            "selected_window": queries.normalize_window(args[-1] if isinstance(args[-1], str) else None),
            "window_context": {"label": "June 2026 · through 30 Jun"},
            "search": "central",
            "date_nav": {"previous": None, "next": None},
            "stop_list": [{"stop_group_id": "1001", "stop_group_name": "Central", "modes_served": "bus"}],
            "picker_pagination": {"page": 3, "first_item": 25, "has_previous": True, "has_next": True},
            "stop_landing_summary": {},
            "stop_landing_rows": [],
            "pagination": {"page": 2, "first_item": 21, "has_previous": True, "has_next": True},
        }

    monkeypatch.setattr(queries, "get_stops", fake_get_stops)
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _path: True)

    client = create_app().test_client()
    response = client.get(
        "/stops/?q=central&page=2&picker_page=3&date=2026-06-30&window=month",
        headers={"HX-Request": "true"},
    )

    assert response.status_code == HTTPStatus.OK
    assert re.search(rb'href="/static/site\.css\?v=[0-9a-f]{12}"', response.data)
    assert captured == {"page": "2", "picker_page": "3", "window": "month", "date": "2026-06-30"}
    assert b'href="/lines/?date=2026-06-30&amp;window=month"' in response.data
    assert re.search(rb'class="active" href="[^"]*window=month[^"]*">month</a>', response.data)
    assert b'<input type="hidden" name="window" value="month">' in response.data
    assert b"picker_page=3" in response.data
    assert b'rel="prev">&lt;</a>' in response.data
    assert b'rel="next">&gt;</a>' in response.data
    assert b"\xe2\x86\x90 previous" not in response.data
    assert b"next \xe2\x86\x92" not in response.data

    refreshed_response = client.get("/stops/?q=central&page=2&picker_page=3&date=2026-06-30")

    assert refreshed_response.status_code == HTTPStatus.OK
    assert captured["date"] == "2026-06-30"
    assert captured["window"] is None
    assert b'href="/lines/?date=2026-06-30"' in refreshed_response.data
