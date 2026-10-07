# ruff: noqa: PLR2004 - literal seconds and counts are behavioral expectations
from __future__ import annotations

import json
import re
from datetime import date, datetime, timedelta
from html import unescape
from http import HTTPStatus
from pathlib import Path
from typing import TYPE_CHECKING, Any

import duckdb

from ztm_frontend import journey, planner, planner_text, queries
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
                ('3003', 'Marymont-Potok', 'marymont-potok', ['N50'], 10),
                ('4004', 'Centrum', 'centrum', ['M1'], 100)
            ) t(stop_group_id, name, search_key, lines, visits);

            create table planner_trip as select * from (values
                (1::bigint, date '2026-09-23', 'bus', '110', 'Metro Marymont', 'D1', '3', 'S1'),
                (-5::bigint, date '2026-09-23', 'bus', '110', 'Metro Marymont', 'D1', '3', 'S1'),
                (7::bigint, date '2026-09-22', 'bus', 'N50', 'Metro Marymont', 'D2', '1', null),
                (10::bigint, date '2026-09-23', 'metro', 'M1', 'Centrum', null, null, null),
                (11::bigint, date '2026-09-23', 'metro', 'M1', 'Centrum', null, null, null)
            ) t(trip_key, service_date, mode, line, headsign, duty_id, brigade, shape_id);

            create table planner_stop as select * replace (ride_from_start_s::double as ride_from_start_s),
                true as can_alight, null::integer as shape_dist_m from (values
                -- 07:30 -> 07:50 scheduled; usually 60 s late at boarding; predicted ride 25 min
                (1::bigint, 0, '100103', '1001', 'Łomianki', 27000, 60, 180, -30, 0.0, 27060),
                (1::bigint, 1, '200201', '2002', 'Metro Marymont', 28200, 90, 240, null, 1500.0, 28560),
                (-5::bigint, 0, '100103', '1001', 'Łomianki', 30600, 0, 60, 0, 0.0, 30600),
                (-5::bigint, 1, '200201', '2002', 'Metro Marymont', 31800, 0, 60, null, 1200.0, 31800),
                -- previous service date, 25:10 = 01:10 on the 23rd; the second stop's table implies a later start
                (7::bigint, 0, '100101', '1001', 'Łomianki', 90600, 0, 30, 0, 0.0, 90600),
                (7::bigint, 1, '300301', '3003', 'Marymont-Potok', 91200, 0, 30, 0, 500.0, 91150),
                (7::bigint, 2, '200202', '2002', 'Metro Marymont', 91800, 0, 30, null, 1000.0, 91650),
                -- 08:04 is too early for the bus's 08:03 bound plus a 2 min walk; 08:06 is catchable.
                -- Metro has zero delay and timetable ride durations, not a bus model spread.
                (10::bigint, 0, '200203', '2002', 'Metro Marymont', 29040, 0, 0, 0, 0.0, 29040),
                (10::bigint, 1, '400401', '4004', 'Centrum', 29760, 0, 0, null, 720.0, 29760),
                (11::bigint, 0, '200203', '2002', 'Metro Marymont', 29160, 0, 0, 0, 0.0, 29160),
                (11::bigint, 1, '400401', '4004', 'Centrum', 29880, 0, 0, null, 720.0, 29880)
            ) t(trip_key, stop_sequence, stop_id, stop_group_id, stop_name, scheduled_sod,
                usual_delay_s, late_delay_s, leave_by_offset_s, ride_from_start_s, expected_sod);

            create table planner_range as select false as is_tram, true as weekday, 7::integer as hour,
                0.0::double as min_ride_s, 1e9::double as max_ride_s,
                0.9::double as low_ratio, 1.2::double as high_ratio;

            create table planner_footpath as select * from (values
                ('200201', '200203', 120, 120)
            ) t(from_stop_id, to_stop_id, distance_m, walk_s);

            create table planner_stop_post as select stop_id, lat::double as lat, lon::double as lon from (values
                ('100101', 52.33, 20.92), ('100103', 52.331, 20.921), ('200201', 52.27, 20.97),
                ('200202', 52.271, 20.971), ('200203', 52.272, 20.972), ('300301', 52.29, 20.95),
                ('400401', 52.23, 21.01)
            ) t(stop_id, lat, lon);

            create table planner_shape as select 'S1' as shape_id, [52.331, 52.27]::double[] as lat,
                [20.921, 20.97]::double[] as lon, [0, 7000]::integer[] as dist_m;

            create table planner_live_persistence (is_tram boolean, horizon_min integer, excess_s integer,
                alpha double, low_s double, mid_s double, high_s double);
            create table planner_live_turnaround (is_tram boolean, low_s double, mid_s double, high_s double);
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
    client = create_app().test_client()
    client.environ_base["HTTP_ACCEPT_LANGUAGE"] = "en"  # page assertions read the English wording
    return client


def _assert_lazy_stops(client: FlaskClient, page: str, expected: list[tuple[int, str, str]]) -> None:
    requests = re.findall(r'hx-get="([^"]+)"\s+hx-trigger="toggle once" hx-target="#([^"]+)" hx-swap="innerHTML"', page)
    assert len(requests) == len(expected)
    targets = [target for _, target in requests]
    assert len(set(targets)) == len(targets)
    for (url, target), (trip_key, board_name, alight_name) in zip(requests, expected, strict=True):
        assert page.count(f'id="{target}"') == 1
        assert unescape(url) == f"/planner/trip/{trip_key}?date=2026-09-23&board=0&alight=1"
        response = client.get(unescape(url))
        assert response.status_code == HTTPStatus.OK
        stops = response.get_data(as_text=True)
        assert board_name in stops
        assert alight_name in stops


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


def _kinds(card: dict[str, Any]) -> list[str]:
    return [item["kind"] for item in card["timeline"]]


def _items(card: dict[str, Any], kind: str) -> list[dict[str, Any]]:
    return [item for item in card["timeline"] if item["kind"] == kind]


def test_connections_and_page_use_router_timings(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    results = planner.connections(path, "1001", "2002", DAY, 7 * 3600)
    first, second = results
    assert _kinds(first) == ["stop", "ride", "stop"]
    board, ride, alight = first["timeline"]
    assert (first["changes"], first["initial_walk"]) == (0, False)
    assert (ride["trip_key"], ride["minutes"], ride["stop_count"]) == (1, 25, 1)
    assert (board["name"], board["post"], first["from_name"], first["from_post"]) == (
        "Łomianki",
        "03",
        "Łomianki",
        "03",
    )
    assert planner.clock(board["depart"]) == "07:31"  # 07:30 + usual 60 s
    assert first["depart"] == 7 * 3600 + 29 * 60 + 30  # exact router deadline, not expected departure
    assert planner.clock(first["leave_by"]) == planner.clock(board["be_by"]) == "07:29"  # rounded down: never late
    assert planner.clock(first["arrive"]) == planner.clock(alight["arrive"]) == "07:56"
    # Boarding spread (120 s) + ride high ratio 1.2 (300 s), not the alighting stop's delay.
    assert planner.clock(first["arrive_by"]) == "08:03"
    assert (alight["arrive_differs"], board["depart_differs"]) == (True, False)
    assert "wait_minutes" not in board  # the first boarding is not a change
    assert first["chips"] == [{"kind": "ride", "mode": "bus", "line": "110"}]
    assert _items(second, "ride")[0]["trip_key"] == NEGATIVE_TRIP_KEY
    assert planner.clock(second["depart"]) == "08:30"
    # The 08:30 ride has no calibrated cell: 60 s boarding spread + 20 min * fallback 1.1.
    assert planner.clock(second["arrive"]) == "08:50"
    assert planner.clock(second["arrive_by"]) == "08:53"

    page = planner.get_page(path, {"from": "1001", "to": "2002", "date": DAY.isoformat(), "time": "07:00"}, DAY, 0)
    assert page["results"] == results
    assert (page["earlier_time"], page["later_time"]) == ("06:30", "08:31")
    at_deadline = planner.connections(path, "1001", "2002", DAY, first["depart"])
    after_deadline = planner.connections(path, "1001", "2002", DAY, first["depart"] + 1)
    assert at_deadline == results
    assert after_deadline == [second]


def test_post_labels_name_the_post_metro_or_train() -> None:
    assert [planner.post_label(s) for s in ("512301", "7013M:P1", "2900")] == ["01", "metro", "train"]


def test_night_trips_from_the_previous_service_date_are_found(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    night = planner.connections(path, "1001", "2002", DAY, 3600)[0]
    board, ride, _ = night["timeline"]
    assert (ride["line"], planner.clock(board["depart"]), ride["stop_count"]) == ("N50", "01:10", NIGHT_STOP_COUNT)
    assert planner.clock(night["arrive"]) == "01:28"
    assert planner.clock(night["arrive_by"]) == "01:29"
    assert planner.connections(path, "2002", "1001", DAY, 0) == []  # wrong direction


def test_a_trip_has_the_same_expected_times_from_every_boarding_stop(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    net = journey.network(path, DAY)
    # Anchored at its own boarding stop, each ride would reach Metro Marymont at 01:26:40 or 01:28:20.
    assert net.timing(7, 0, 2).arrive == net.timing(7, 1, 2).arrive == 91650 - 86400
    from_first, from_second = (planner.trip_stops(path, 7, board, 2, DAY) for board in (0, 1))
    assert [planner.clock(stop["expected"]) for stop in from_first] == ["01:10", "01:19", "01:28"]
    # Boarded at Marymont-Potok, the bus leaves no earlier than the boarding deadline there.
    assert [planner.clock(stop["expected"]) for stop in from_second] == ["01:20", "01:28"]


def test_planner_page_renders_cards_and_trip_stops(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    page = client.get("/planner?date=2026-09-23&time=07:00&from=1001&to=2002").get_data(as_text=True)
    assert 'href="/planner"' in page
    assert page.count('class="pl-card"') == len(
        planner.connections(tmp_path / "planner" / "planner.duckdb", "1001", "2002", DAY, 7 * 3600)
    )
    assert '<span class="pl-time mono">07:29</span>' in page  # be at the stop
    assert 'be here by <b class="mono">07:29</b>' in page
    assert 'running late: <b class="mono">08:03</b>' in page
    assert "9 times in 10" not in page  # a whole journey's bound is not a calibrated 90%
    assert 'title="timetable 07:50"' in page  # arrival flagged against the timetable
    assert 'class="landing-line-pill mono pl-pill mode-bus" title="bus">110</span>' in page
    assert "Later routes" in page
    assert "/planner/trip/-5?date=2026-09-23&amp;board=0&amp;alight=1" in page
    _assert_lazy_stops(client, page, [(1, "Łomianki", "Metro Marymont"), (-5, "Łomianki", "Metro Marymont")])

    stops = client.get("/planner/trip/-5?date=2026-09-23&board=0&alight=1")
    assert stops.status_code == HTTPStatus.OK
    assert "Metro Marymont" in stops.get_data(as_text=True)
    assert client.get("/planner/trip/-5?date=2026-09-23").status_code == HTTPStatus.NOT_FOUND
    assert client.get("/planner/trip/99?date=2026-09-23&board=0&alight=1").status_code == HTTPStatus.NOT_FOUND


def test_connection_uses_late_bus_bound_and_walk_to_choose_metro(tmp_path: Path) -> None:
    path = tmp_path / "planner.duckdb"
    _write_artifact(path)
    (result,) = planner.connections(path, "1001", "4004", DAY, 7 * 3600)
    assert _kinds(result) == ["stop", "ride", "stop", "walk", "stop", "ride", "stop"]
    _, bus, alight, walk, change, metro, final = result["timeline"]
    assert (bus["trip_key"], metro["trip_key"]) == (1, 11)  # not the uncatchable 08:04 trip 10
    assert (result["changes"], result["initial_walk"]) == (1, False)
    assert planner.clock(result["leave_by"]) == "07:29"
    assert (alight["stop_id"], change["stop_id"]) == ("200201", "200203")
    assert walk == {"kind": "walk", "minutes": 2, "distance_m": 120}
    assert [c["kind"] for c in result["chips"]] == ["ride", "walk", "ride"]
    # Late bus (08:03) + 2 min walk still makes the 08:06 metro.
    assert planner.clock(change["be_by"]) == "08:06"
    assert planner.clock(change["arrive"]) == "07:58"  # expected bus arrival 07:56 + walk
    assert change["wait_minutes"] == 8
    assert (metro["mode"], metro["line"], metro["minutes"]) == ("metro", "M1", 12)
    assert planner.clock(change["depart"]) == "08:06"
    # Metro uses the timetable, with no bus ride-range multiplier or delay model.
    assert planner.clock(result["arrive"]) == planner.clock(final["arrive"]) == "08:18"
    assert planner.clock(result["arrive_by"]) == "08:18"
    page = planner.get_page(path, {"from": "1001", "to": "4004", "time": "07:00"}, DAY, 0)
    assert page["results"] == [result]
    assert page["later_time"] == "07:30"  # advances past 07:29:30, not the 07:31 expected departure


def test_multi_leg_page_renders_changes_walks_and_distinct_lazy_stops(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch)
    response = client.get("/planner?date=2026-09-23&time=07:00&from=1001&to=4004")
    assert response.status_code == HTTPStatus.OK
    page = response.get_data(as_text=True)
    assert page.count('class="pl-card"') == 1
    assert 'pl-pill mode-metro" title="metro">M1</span>' in page
    assert "1 change" in page
    assert "walk 2 min · 120 m" in page
    assert "wait 8 min" in page
    assert 'be here by <b class="mono">08:06</b>' not in page  # the timetabled metro: be there when it leaves
    assert "Changes leave room for a late arrival and the walk." in page
    _assert_lazy_stops(client, page, [(1, "Łomianki", "Metro Marymont"), (11, "Metro Marymont", "Centrum")])
    assert 'hx-target="#pl-trip-1-2"' in page
    assert 'hx-target="#pl-trip-1-6"' in page  # the intervening walk has no lazy trip request
    assert "/planner/trip/10?" not in page


def test_endpoint_walks_render_start_deadline_and_final_arrival(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch)
    path = tmp_path / "planner" / "planner.duckdb"
    with duckdb.connect(str(path)) as connection:
        connection.execute(
            "insert into planner_footpath values ('100103', '200203', 720, 600), ('400401', '300301', 120, 120)"
        )
    state = planner.get_page(path, {"from": "1001", "to": "3003", "time": "07:50"}, DAY, 0)
    first, second = state["results"]
    assert _kinds(first) == ["stop", "walk", "stop", "ride", "stop", "walk", "stop"]
    start, initial, board, metro, alight, final_walk, end = first["timeline"]
    assert (first["initial_walk"], first["changes"]) == (True, 0)
    assert (start["name"], board["name"], alight["name"], end["name"]) == (
        "Łomianki", "Metro Marymont", "Centrum", "Marymont-Potok",
    )  # fmt: skip
    assert (first["from_name"], first["from_post"]) == ("Metro Marymont", "03")  # where the first ride starts
    assert (initial["minutes"], final_walk["minutes"]) == (10, 2)
    assert metro["trip_key"] == 10
    assert (
        planner.clock(first["depart"]) == planner.clock(first["leave_by"]) == planner.clock(start["depart"]) == "07:54"
    )
    assert planner.clock(board["be_by"]) == "08:04"
    assert "wait_minutes" not in board  # walking to the first ride is not a change
    assert planner.clock(alight["arrive"]) == "08:16"
    assert (
        planner.clock(first["arrive"]) == planner.clock(first["arrive_by"]) == planner.clock(end["arrive"]) == "08:18"
    )
    assert planner.clock(second["leave_by"]) == "07:56"
    assert planner.clock(second["arrive_by"]) == "08:20"

    response = client.get("/planner?date=2026-09-23&time=07:50&from=1001&to=3003")
    assert response.status_code == HTTPStatus.OK
    page = response.get_data(as_text=True)
    assert page.count("Leave at") == 2
    assert '<span class="pl-time mono">07:54</span>' in page
    assert "walk 10 min · 720 m" in page
    assert "running late" not in page  # under 2 min past the expected arrival
    _assert_lazy_stops(client, page, [(10, "Metro Marymont", "Centrum"), (11, "Metro Marymont", "Centrum")])


def test_page_keeps_one_generation_when_artifact_is_published_mid_request(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch)
    path = tmp_path / "planner" / "planner.duckdb"
    replacement = tmp_path / "replacement.duckdb"
    _write_artifact(replacement)
    with duckdb.connect(str(replacement)) as connection:
        connection.execute("update planner_metadata set build_id = 'b2'")
        connection.execute("update planner_trip set trip_key = 21 where trip_key = 1")
        connection.execute("update planner_stop set trip_key = 21 where trip_key = 1")
    original = planner.search

    def publish_then_search(  # noqa: PLR0913
        artifact: Path, origin: str, destination: str, day: date, after_sod: int, now: datetime | None = None
    ) -> tuple[list[dict[str, Any]], int | None]:
        # Date/stop lookups already opened the old request-scoped connection.
        replacement.replace(path)
        monkeypatch.setattr(planner, "search", original)
        return original(artifact, origin, destination, day, after_sod, now)

    monkeypatch.setattr(planner, "search", publish_then_search)
    url = "/planner?date=2026-09-23&time=07:00&from=1001&to=2002"
    old = client.get(url)
    new = client.get(url)
    assert old.status_code == new.status_code == HTTPStatus.OK
    assert "/planner/trip/1?" in old.get_data(as_text=True)
    assert "/planner/trip/21?" not in old.get_data(as_text=True)
    assert "/planner/trip/21?" in new.get_data(as_text=True)
    assert "/planner/trip/1?" not in new.get_data(as_text=True)


def test_suggestions_keep_the_rest_of_the_form(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    html = client.get("/planner/suggest/to?q_to=metro&from=1001&date=2026-09-23&time=07:00").get_data(as_text=True)
    assert "/planner?date=2026-09-23&amp;time=07:00&amp;from=1001&amp;to=2002" in html
    assert client.get("/planner/suggest/via?q_via=metro").status_code == HTTPStatus.NOT_FOUND


def test_planner_ignores_unknown_query_keys(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    client = _client(tmp_path, monkeypatch)
    response = client.get("/planner?from=1001&endpoint=x&_external=1&_method=POST")
    assert response.status_code == HTTPStatus.OK
    assert 'href="/planner?from=1001&amp;lang=pl"' in response.get_data(as_text=True)


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
            columns = [(row[0], row[1]) for row in connection.execute(f"describe {table}").fetchall()]
            assert columns == [(field["name"], field["duckdb_type"]) for field in fields], table


def test_cards_beaten_on_usual_times_are_dropped() -> None:
    def card(leave_by: int, changes: int, arrive: int) -> tuple[int, int, int]:
        return leave_by, changes, arrive

    direct, via_metro, later = card(600, 0, 2280), card(600, 1, 2400), card(1500, 1, 3000)
    assert planner._beats(direct, via_metro)  # same start, fewer changes, earlier usual arrival  # noqa: SLF001
    assert not planner._beats(direct, later)  # noqa: SLF001 - leaving later is its own option
    assert not planner._beats(direct, card(600, 0, 2280))  # noqa: SLF001 - equal cards both stay


def test_planner_speaks_polish_to_polish_browsers_and_remembers_the_switch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client = _client(tmp_path, monkeypatch)
    url = "/planner?date=2026-09-23&time=07:00&from=1001&to=4004"
    page = client.get(url, headers={"Accept-Language": "pl-PL,pl;q=0.9,en;q=0.8"}).get_data(as_text=True)
    assert '<main class="main pl-main" lang="pl">' in page
    assert "1 przesiadka" in page
    assert "pieszo 2 min · 120 m" in page
    assert "Przesiadki mają zapas na spóźnienie pojazdu i dojście." in page
    assert "Szukaj" in page
    assert 'href="/planner?date=2026-09-23&amp;time=07:00&amp;from=1001&amp;to=4004&amp;lang=en"' in page

    switched = client.get(url + "&lang=pl")
    assert "planner_lang=pl" in switched.headers["Set-Cookie"]
    assert "1 przesiadka" in client.get(url).get_data(as_text=True)  # the cookie beats the browser's English
    stops = client.get("/planner/trip/11?date=2026-09-23&board=0&alight=1").get_data(as_text=True)
    assert "przewidywany" in stops


def test_polish_plurals_and_eu_dates() -> None:
    forms = ("przesiadka", "przesiadki", "przesiadek")
    assert [planner_text.plural(n, forms) for n in (1, 2, 4, 5, 12, 22, 25)] == [
        "przesiadka", "przesiadki", "przesiadki", "przesiadek", "przesiadek", "przesiadki", "przesiadek",
    ]  # fmt: skip
    assert planner_text.plural(2, ("stop", "stops")) == "stops"
    assert planner_text.day_label(DAY, DAY, "pl") == "Dziś, śr. 23.09"
    assert planner_text.day_label(DAY + timedelta(days=2), DAY, "en") == "Fri 25 Sep"


def test_later_page_starts_where_an_unshown_journey_would_be_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    def found(*departs: int) -> None:
        legs = (journey.Walk("a", "b", 60),)
        results = [journey.Journey(d, d + 600, legs) for d in departs]
        monkeypatch.setattr(planner.journey, "plan", lambda *_args, **_kwargs: results)

    monkeypatch.setattr(planner.journey, "network", lambda *_args: None)
    monkeypatch.setattr(planner, "_connection", lambda _path, result, _day: {"depart": result.depart})
    shown = [600 + 60 * i for i in range(planner.RESULTS)]  # the last card leaves at 00:15:00
    found(*shown, shown[-1] + 30)  # an unshown journey at 00:15:30
    assert planner.search(Path(), "1", "2", DAY, 0)[1] == shown[-1]
    found(*shown, shown[-1] + 60)
    assert planner.search(Path(), "1", "2", DAY, 0)[1] == shown[-1] + 60
    found(*[600] * (planner.RESULTS + 1))  # a whole page in the requested minute still advances
    assert planner.search(Path(), "1", "2", DAY, 600)[1] == 660
