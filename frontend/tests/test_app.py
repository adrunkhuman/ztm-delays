from __future__ import annotations

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
    assert b"observed minutes" in response.data
    assert b"Bus partial" in response.data
    assert b"matched" not in response.data


def test_stop_page_preserves_independent_picker_page(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    def fake_get_stops(*args: object) -> dict[str, object]:
        captured["page"] = args[-2]
        captured["picker_page"] = args[-1]
        return {
            "selected_stop_group_id": None,
            "selected_mode": "bus",
            "selected_rank": "worst",
            "selected_date": "2026-06-30",
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

    response = create_app().test_client().get("/stops/?q=central&page=2&picker_page=3")

    assert response.status_code == HTTPStatus.OK
    assert captured == {"page": "2", "picker_page": "3"}
    assert b"picker_page=3" in response.data
    assert b'rel="prev">&lt;</a>' in response.data
    assert b'rel="next">&gt;</a>' in response.data
    assert b"\xe2\x86\x90 previous" not in response.data
    assert b"next \xe2\x86\x92" not in response.data
