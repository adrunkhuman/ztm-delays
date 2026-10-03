from __future__ import annotations

import json
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
    from flask.testing import FlaskClient


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
    github_link = b'href="https://github.com/adrunkhuman/ztm-delays"'
    assert github_link in response.data
    github_anchor = response.data.split(github_link, 1)[1].split(b"</a>", 1)[0]
    assert b'aria-label="Project on GitHub"' in github_anchor
    assert b"<svg " in github_anchor
    assert b'aria-hidden="true"' in github_anchor
    assert response.data.index(github_link) < response.data.index(b">status</a>")
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


def _write_map_month(maps_dir: Path, month: str, segments: dict[str, dict[str, object]]) -> None:
    totals = {"corridors": 1, "traversals": 1}
    meta = {
        "anchor": f"{month}-30",
        "totals": {period: {"bus": totals, "tram": totals} for period in ("weekday", "weekend")},
        "tram_bounds": [[20.9, 52.1], [21.1, 52.3]],
    }
    (maps_dir / month).mkdir(parents=True)
    (maps_dir / month / "segments.json").write_text(json.dumps({"meta": meta, "segments": segments}))
    (maps_dir / month / "routes.geojson").write_text("{}")


def _map_version(maps_dir: Path, month: str) -> int:
    return (maps_dir / month / "segments.json").stat().st_mtime_ns


def _map_client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> FlaskClient:
    monkeypatch.setenv("ZTM_MAPS_DIR", str(tmp_path))
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _path: True)
    return create_app().test_client()


def test_route_map_selects_published_month_and_view(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _map_client(tmp_path, monkeypatch)
    assert client.get("/map").status_code == HTTPStatus.NOT_FOUND
    assert 'href="/map"' not in client.get("/status").get_data(as_text=True)

    _write_map_month(tmp_path, "2026-08", {})
    _write_map_month(tmp_path, "2026-09", {})
    (tmp_path / "2026-10").mkdir()  # Unpublished: no segments.json yet.

    newest = client.get("/map?mode=tram&period=we").get_data(as_text=True)
    assert "<b>2026-09</b>" in newest
    assert 'href="/map?month=2026-08&amp;mode=tram&amp;period=we"' in newest
    version = _map_version(tmp_path, "2026-09")
    assert f'data-routes="/map/2026-09/routes.geojson?v={version}"' in newest
    assert f'data-segments="/map/segments?month=2026-09&amp;v={version}"' in newest
    assert 'href="/map"' in newest
    assert 'data-mode="tram" data-period="we"' in newest

    older = client.get("/map?month=2026-08&mode=nope").get_data(as_text=True)
    assert "<b>2026-08</b>" in older
    assert 'data-mode="bus" data-period="wd"' in older
    assert 'href="/map?month=2026-09&amp;mode=bus&amp;period=wd"' in older


def test_route_map_files_are_limited_to_published_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _map_client(tmp_path, monkeypatch)
    _write_map_month(tmp_path, "2026-09", {})

    assert client.get("/map/2026-09/routes.geojson").status_code == HTTPStatus.OK
    assert client.get("/map/2026-09/segments.json").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/map/2026-10/routes.geojson").status_code == HTTPStatus.NOT_FOUND


def test_route_map_popup_pages_view_segments_by_traversals(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _map_client(tmp_path, monkeypatch)

    def segment(stops: str, count: int, delta: float) -> dict[str, object]:
        return {
            "stops": stops,
            "aliases": 0,
            "bus": ["N01"],
            "wd_bus_d": delta,
            "wd_bus_n": count,
            "wd_bus_g": count,
            "wd_bus_r": 0,
        }

    _write_map_month(
        tmp_path,
        "2026-09",
        {
            "1": segment("Quiet", 100, 0.4),
            "2": segment("Busy", 900, 64.6),
            "3": segment("Middle", 500, -31),
            "4": segment("Busier", 950, 12),
            "5": {"stops": "Weekend only", "aliases": 0, "we_bus_d": 5, "we_bus_n": 999, "we_bus_g": 0, "we_bus_r": 0},
        },
    )

    url = f"/map/segments?month=2026-09&v={_map_version(tmp_path, '2026-09')}&mode=bus&period=wd"
    first = client.get(f"{url}&ids=1,2,3,4,5,2").get_data(as_text=True)
    assert [int(value) for value in re.findall(r'data-id="(\d+)"', first)] == [4, 2, 3]
    assert "+65s" in first
    assert "1\u20133 / 4" in first
    assert "page=1" in first
    assert f"v={_map_version(tmp_path, '2026-09')}" in first

    last = client.get(f"{url}&ids=1,2,3,4,5&page=9").get_data(as_text=True)
    assert re.findall(r'data-id="(\d+)"', last) == ["1"]
    assert "4\u20134 / 4" in last

    assert client.get(f"{url}&ids=5").status_code == HTTPStatus.NOT_FOUND
    assert client.get(f"{url}&ids=%C2%B2,{'9' * 5000}").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/map/segments?month=2026-10&ids=1").status_code == HTTPStatus.NOT_FOUND
    # A page loaded before a rebuild carries the old version; its ids may now name other corridors.
    stale = client.get("/map/segments?month=2026-09&v=0&mode=bus&period=wd&ids=1").get_data(as_text=True)
    assert "Reload the page" in stale
    assert "data-id" not in stale
