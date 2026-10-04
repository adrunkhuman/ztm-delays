from __future__ import annotations

import json
from datetime import date
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING

import duckdb

from ztm_frontend import planner, queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    import pytest
    from flask.testing import FlaskClient

CONTRACT = Path(__file__).resolve().parents[2] / "contracts" / "planner_artifact_v1.json"
DAY = date(2026, 9, 23)  # a Wednesday
NEGATIVE_TRIP_KEY = -5  # trip keys are signed 64-bit hashes
NIGHT_STOP_COUNT = 2


def _write_artifact(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            """
            create table planner_metadata as select 'b1' as build_id, timestamp '2026-09-22 03:00' as built_at,
                'test' as model_version, date '2026-09-22' as first_date, date '2026-09-24' as last_date;

            create table planner_stop_group as select * from (values
                ('1001', 'Łomianki', 'lomianki', ['110'], 50),
                ('2002', 'Metro Marymont', 'metro marymont', ['110', 'N50'], 80),
                ('3003', 'Marymont-Potok', 'marymont-potok', ['N50'], 10)
            ) t(stop_group_id, name, search_key, lines, visits);

            create table planner_trip as select * from (values
                (1::bigint, date '2026-09-23', 'bus', '110', 'Metro Marymont'),
                (-5::bigint, date '2026-09-23', 'bus', '110', 'Metro Marymont'),
                (7::bigint, date '2026-09-22', 'bus', 'N50', 'Metro Marymont')
            ) t(trip_key, service_date, mode, line, headsign);

            create table planner_stop as select * from (values
                -- 07:30 -> 07:50 scheduled; usually 60 s late at boarding; predicted ride 25 min
                (1::bigint, 0, '100103', '1001', 'Łomianki', 27000, 60, 180, -30, 0.0),
                (1::bigint, 1, '200201', '2002', 'Metro Marymont', 28200, 90, 240, null, 1500.0),
                (-5::bigint, 0, '100103', '1001', 'Łomianki', 30600, 0, 60, 0, 0.0),
                (-5::bigint, 1, '200201', '2002', 'Metro Marymont', 31800, 0, 60, null, 1200.0),
                -- previous service date, 25:10 = 01:10 on the 23rd
                (7::bigint, 0, '100101', '1001', 'Łomianki', 90600, 0, 30, 0, 0.0),
                (7::bigint, 1, '300301', '3003', 'Marymont-Potok', 91200, 0, 30, 0, 500.0),
                (7::bigint, 2, '200202', '2002', 'Metro Marymont', 91800, 0, 30, null, 1000.0)
            ) t(trip_key, stop_sequence, stop_id, stop_group_id, stop_name, scheduled_sod,
                usual_delay_s, late_delay_s, leave_by_offset_s, ride_from_start_s);

            create table planner_range as select false as is_tram, true as weekday, 7::tinyint as hour,
                0.0 as min_ride_s, 1e9 as max_ride_s, 0.9 as low_ratio, 1.2 as high_ratio;
            """
        )


def _client(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, *, published: bool = True) -> FlaskClient:
    artifact = tmp_path / "planner" / "planner.duckdb"
    if published:
        _write_artifact(artifact)
    monkeypatch.setenv("ZTM_PLANNER_PATH", str(artifact))
    monkeypatch.setenv("ZTM_MAPS_DIR", str(tmp_path / "maps"))
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    monkeypatch.setattr(queries, "grouped_windows_available", lambda _path: True)
    return create_app().test_client()


def test_planner_is_hidden_until_an_artifact_is_published(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch, published=False)
    assert client.get("/planner").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/planner/suggest/from?q_from=lom").status_code == HTTPStatus.NOT_FOUND


def test_stop_search_ignores_accents_and_prefers_exact_names(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    assert [g["name"] for g in planner.suggest(path, "ŁOMIA")] == ["Łomianki"]
    assert [g["name"] for g in planner.suggest(path, "marymont")] == ["Marymont-Potok", "Metro Marymont"]
    assert planner.suggest(path, "metro marymont")[0]["name"] == "Metro Marymont"
    assert planner.suggest(path, "m") == []
    # Typed text that no longer names the chosen group wins over the stale hidden id.
    retyped, kept = (
        planner.resolve_stop_group(path, "1001", "potok"),
        planner.resolve_stop_group(path, "1001", "Lomianki"),
    )
    assert retyped is not None
    assert retyped["stop_group_id"] == "3003"
    assert kept is not None
    assert kept["stop_group_id"] == "1001"


def test_departure_times_use_usual_delay_and_the_calibrated_range(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    first, second = planner.departures(path, "1001", "2002", DAY, 7 * 3600)
    assert first["trip_key"] == 1
    assert planner.clock(first["depart"]) == "07:31"  # 07:30 + usual 60 s
    assert planner.clock(first["leave_by"]) == "07:29"  # 07:29:30 rounded down: never later than safe
    assert planner.clock(first["arrive"]) == "07:56"  # 07:31 + 25 min ride
    # + late-vs-usual departure spread (120 s) + ride high ratio 1.2 (300 s) = 08:03, rounded up
    assert planner.clock(first["arrive_by"]) == "08:03"
    assert (first["ride_minutes"], first["timetable_minutes"]) == (25, 20)
    # Expected 07:56 vs timetable 07:50 gets a marker; leaving 1 min late does not.
    assert (first["arrive_differs"], first["depart_differs"]) == (True, False)
    assert second["trip_key"] == NEGATIVE_TRIP_KEY
    assert second["high_ratio"] == planner.DEFAULT_HIGH_RATIO  # 08:30 has no calibrated cell in this fixture


def test_night_trips_from_the_previous_service_date_are_found(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    night = planner.departures(path, "1001", "2002", DAY, 3600)[0]
    assert (night["line"], planner.clock(night["depart"]), night["stop_count"]) == ("N50", "01:10", NIGHT_STOP_COUNT)
    assert planner.departures(path, "2002", "1001", DAY, 0) == []  # wrong direction


def test_planner_page_renders_cards_and_trip_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    page = client.get("/planner?date=2026-09-23&time=07:00&from=1001&to=2002").get_data(as_text=True)
    assert 'href="/planner"' in page
    assert page.count('class="pl-card"') == len(
        planner.departures(tmp_path / "planner" / "planner.duckdb", "1001", "2002", DAY, 7 * 3600)
    )
    assert '<span class="pl-time mono">07:29</span>' in page  # be at the stop
    assert '9 times in 10 you\'ll arrive by <b class="mono">08:03</b>' in page
    assert 'title="timetable 07:50"' in page  # arrival flagged against the timetable
    assert "Later routes" in page
    assert "/planner/trip/-5?date=2026-09-23&amp;board=0&amp;alight=1" in page

    stops = client.get("/planner/trip/-5?date=2026-09-23&board=0&alight=1")
    assert stops.status_code == HTTPStatus.OK
    assert "Metro Marymont" in stops.get_data(as_text=True)
    assert client.get("/planner/trip/-5?date=2026-09-23").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/planner/trip/99?date=2026-09-23&board=0&alight=1").status_code == HTTPStatus.NOT_FOUND


def test_suggestions_keep_the_rest_of_the_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    html = client.get("/planner/suggest/to?q_to=metro&from=1001&date=2026-09-23&time=07:00").get_data(as_text=True)
    assert "/planner?date=2026-09-23&amp;time=07:00&amp;from=1001&amp;to=2002" in html
    assert client.get("/planner/suggest/via?q_via=metro").status_code == HTTPStatus.NOT_FOUND


def test_page_state_falls_back_inside_the_published_window(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    outside = planner.get_page(path, {"date": "2026-12-01", "time": "25:99"}, date(2026, 12, 1), 8 * 3600)
    assert (outside["day"], outside["time"], outside["results"]) == (date(2026, 9, 22), "08:00", [])
    same = planner.get_page(path, {"from": "1001", "to": "1001"}, DAY, 0)
    assert same["results"] == []


def test_fixture_follows_the_artifact_contract(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
    with duckdb.connect(str(path)) as connection:
        for table, fields in contract["tables"].items():
            columns = [row[0] for row in connection.execute(f"describe {table}").fetchall()]
            assert columns == [field["name"] for field in fields], table
