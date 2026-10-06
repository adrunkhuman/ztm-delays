from __future__ import annotations

import re
from http import HTTPStatus
from typing import TYPE_CHECKING

import duckdb
import pytest

from tests.test_queries import _create_line_smoke_db
from ztm_frontend import queries
from ztm_frontend.app import create_app

if TYPE_CHECKING:
    from pathlib import Path


@pytest.fixture
def pattern_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "patterns.duckdb"
    _create_line_smoke_db(db_path)
    with duckdb.connect(str(db_path)) as connection:
        connection.execute("""
            insert into mart_line_window_summary
            select * replace ('month' as window_type, '2026-06' as window_key)
            from mart_line_window_summary where mode = 'tram' and window_type = 'day';

            create or replace table mart_line_course_window as
            with patterns as (
                select '1' as line, 'bus' as mode, 0 as direction_id, 'Wiatraczna' as trip_headsign,
                    'month' as window_type, '2026-06' as window_key, date '2026-06-30' as source_end_date,
                    route_pattern_id, pattern_status, origin_stop_name, destination_stop_name,
                    stop_call_count, trip_count, course_rank, observed_service_dates
                from (values
                    ('loop', 'classified', 'Origin', 'Wiatraczna', 5, 6, 1,
                        [date '2026-06-08', date '2026-06-01', date '2026-06-02', date '2026-06-05', date '2026-06-07']),
                    ('alternate', 'classified', 'Origin', 'Wiatraczna', 3, 4, 2,
                        [date '2026-06-14', date '2026-06-15', date '2026-06-16']),
                    ('unclassified', 'unclassified', null, null, null, 2, 3, [date '2026-06-30'])
                ) as rows(route_pattern_id, pattern_status, origin_stop_name, destination_stop_name,
                    stop_call_count, trip_count, course_rank, observed_service_dates)
            )
            select * from patterns
            union all select * replace ('tram' as mode, 99 as trip_count) from patterns
            union all select * replace ('2' as line, 99 as trip_count) from patterns
            union all select * replace (date '2026-06-29' as source_end_date, 99 as trip_count) from patterns
            union all select * replace ('day' as window_type, '2026-06-30' as window_key,
                1 as trip_count, [date '2026-06-30'] as observed_service_dates) from patterns;

            create or replace table mart_line_course_stop_window as
            with calls as (
                select '1' as line, 'bus' as mode, 0 as direction_id, 'Wiatraczna' as trip_headsign,
                    'month' as window_type, '2026-06' as window_key, date '2026-06-30' as source_end_date,
                    route_pattern_id, call_position, call_position as display_rank,
                    stop_group_id, stop_group_id || stop_post_code as stop_id, stop_post_code, stop_post_codes,
                    stop_name, arrival_count, mean_delay_seconds, mean_delay_seconds as median_delay_seconds,
                    mean_delay_seconds as p90_delay_seconds, on_time_rate, delay_histogram
                from (values
                    ('loop', 5, '7002', '01', ['01'], 'Wiatraczna', 6, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 6}]),
                    ('alternate', 3, '7002', '01', ['01'], 'Wiatraczna', 4, 0.0, 0.0,
                        [{'bucket_label': 'early_over_5m', 'n': 4}]),
                    ('loop', 3, '7002', '01', ['01'], 'Wiatraczna', 0, null, null, null),
                    ('loop', 1, '1001', '01', ['01'], 'Origin', 6, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 6}]),
                    ('loop', 2, '7002', '03', ['03', '04'], 'Wiatraczna', 6, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 6}]),
                    ('alternate', 1, '1001', '01', ['01'], 'Origin', 4, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 4}]),
                    ('loop', 4, '1002', '01', ['01'], 'Park', 0, null, null, null),
                    ('alternate', 2, '1003', '02', ['02'], 'Alternate only', 4, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 4}]),
                    ('unclassified', 1, '9999', '01', ['01'], 'Bogus route', 2, 30.0, 1.0,
                        [{'bucket_label': 'on_time_late_0_30s', 'n': 2}])
                ) as rows(route_pattern_id, call_position, stop_group_id, stop_post_code, stop_post_codes,
                    stop_name, arrival_count, mean_delay_seconds, on_time_rate, delay_histogram)
            )
            select * from calls
            union all select * replace ('tram' as mode, 'Tram call' as stop_name) from calls
            union all select * replace ('2' as line, 'Other line call' as stop_name) from calls
            union all select * replace (date '2026-06-29' as source_end_date, 'Stale call' as stop_name) from calls
            union all select * replace ('day' as window_type, '2026-06-30' as window_key,
                'Day call' as stop_name) from calls;
        """)
    return db_path


def test_same_headsign_patterns_keep_ordered_repeated_calls(pattern_db: Path) -> None:
    result = queries.get_lines(pattern_db, "1", "bus", "2026-06-30", None, None, "month")
    loop, alternate, unclassified = result["courses"]

    assert [course["route_pattern_id"] for course in result["courses"]] == ["loop", "alternate", "unclassified"]
    assert {course["trip_headsign"] for course in result["courses"]} == {"Wiatraczna"}
    assert [course["trip_count"] for course in result["courses"]] == [6, 4, 2]
    assert [stop["call_position"] for stop in loop["stops"]] == [1, 2, 3, 4, 5]
    assert [stop["stop_group_id"] for stop in loop["stops"]] == ["1001", "7002", "7002", "1002", "7002"]
    assert loop["stops"][1]["stop_post_codes"] == ["03", "04"]
    assert [stop["stop_name"] for stop in alternate["stops"]] == ["Origin", "Alternate only", "Wiatraczna"]
    assert unclassified["stops"] == []
    [groups] = result["route_columns"]
    assert [group["title"] for group in groups] == ["→ Wiatraczna", "Route unavailable · Wiatraczna"]
    assert groups[0]["courses"] == [loop, alternate]
    assert groups[1]["courses"] == [unclassified]
    assert [family["label"] for family in groups[0]["families"]] == ["Via Wiatraczna", "Via Alternate only"]
    assert [family["main"] for family in groups[0]["families"]] == [loop, alternate]
    assert [family["label"] for family in groups[1]["families"]] == [None]
    assert [stop["show_post_codes"] for stop in loop["stops"]] == [False, True, True, False, True]
    assert not any(stop["show_post_codes"] for stop in alternate["stops"])
    for stop in [loop["stops"][2], loop["stops"][3]]:
        assert stop["arrival_count"] == 0
        assert stop["shape"] is None
        assert stop["mean_delay_seconds"] is None
        assert stop["on_time_rate"] is None


@pytest.mark.parametrize(
    ("mode", "window", "expected_count", "expected_stop"),
    [("bus", "day", 1, "Day call"), ("bus", "month", 6, "Origin"), ("tram", "month", 99, "Tram call")],
)
def test_patterns_preserve_mode_and_window_scope(
    pattern_db: Path, mode: str, window: str, expected_count: int, expected_stop: str
) -> None:
    result = queries.get_lines(pattern_db, "1", mode, "2026-06-30", None, None, window)

    assert result["summary"]["mode"] == mode
    assert result["courses"][0]["trip_count"] == expected_count
    assert result["courses"][0]["stops"][0]["stop_name"] == expected_stop


@pytest.mark.parametrize("missing_histogram", [False, True])
def test_pattern_cards_show_routes_and_unavailable_stats_without_extra_labels(
    pattern_db: Path, monkeypatch: pytest.MonkeyPatch, missing_histogram: bool
) -> None:
    if missing_histogram:
        with duckdb.connect(str(pattern_db)) as connection:
            connection.execute("""
                update mart_line_course_stop_window set delay_histogram = null
                where route_pattern_id = 'alternate' and call_position = 1
            """)
    expected_distributions = 2 if missing_histogram else 3
    expected_card_count = 3
    expected_group_count = 2
    expected_call_count = 5
    expected_post_01_count = 2
    expected_unavailable_stats = 3
    monkeypatch.setenv("ZTM_DUCKDB_PATH", str(pattern_db))
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    response = create_app().test_client().get("/lines/1?mode=bus&date=2026-06-30&window=month")

    assert response.status_code == HTTPStatus.OK
    html = response.get_data(as_text=True)
    cards = re.findall(r'<details class="[^"]*route-pattern[^"]*".*?</details>', html, re.DOTALL)
    assert len(cards) == expected_card_count
    loop, alternate, unclassified = cards
    assert html.count("<span>→ Wiatraczna</span>") == 1
    assert html.count('class="route-group-heading"') == expected_group_count
    assert "2 routes · 10 trips" in html
    assert "Via Wiatraczna" in loop
    assert "route-deviations" not in html
    assert "Via Alternate only" in alternate
    assert "5 stops · 6 trips" in loop
    assert "3 stops · 4 trips" in alternate
    assert all(
        label not in html for label in ("Observed service dates", "scheduled calls", "complete trips", "towards")
    )
    assert "Alternate only" not in loop
    assert loop.count('class="stop-row"') == expected_call_count
    assert "[03, 04]" in loop
    assert loop.count("[01]") == expected_post_01_count
    assert "post-suffix" not in alternate
    rows = re.findall(r'<div class="stop-row">.*?(?=<div class="stop-row">|</details>)', loop, re.DOTALL)
    for row in [rows[2], rows[3]]:
        assert "hist-plot" not in row
        assert row.count("n/a") == expected_unavailable_stats
        assert ">0s<" not in row
        assert ">0%<" not in row
    assert alternate.count("hist-plot") == expected_distributions
    assert ('title="Delay distribution unavailable"' in alternate) == missing_histogram
    assert ">0s<" in alternate
    assert ">0%<" in alternate
    assert "2 trips" in unclassified
    assert html.count("Route unavailable") == 1
    assert "schedule classification" not in unclassified
    assert "stop-row" not in unclassified
    assert "route-columns" not in unclassified
    assert "Bogus route" not in html
    assert "Stale call" not in html
    assert "Tram call" not in html
    assert "Other line call" not in html
    assert "window=month" in loop


def _variant(direction_id: int, trip_count: int, calls: list[tuple[str, str, list[str]]]) -> dict:
    stops = [{"stop_group_id": group, "stop_name": name, "stop_post_codes": posts} for group, name, posts in calls]
    for stop in stops:
        stop["show_post_codes"] = sum(other["stop_group_id"] == stop["stop_group_id"] for other in stops) > 1
    return {
        "direction_id": direction_id,
        "trip_headsign": calls[-1][1],
        "pattern_status": "classified",
        "origin_stop_name": calls[0][1],
        "destination_stop_name": calls[-1][1],
        "route_pattern_id": f"{direction_id}-{trip_count}",
        "stop_call_count": len(stops),
        "trip_count": trip_count,
        "stops": stops,
    }


def test_repeat_call_patterns_nest_under_their_route_family() -> None:
    # Shapes taken from line 143 (Sep 2026): patterns differing only in back-to-back calls at Wiatraczna
    # are one route to riders, while the Gdecka and Pl. Szembeka branches are separate routes.
    lotnika, kolonia, przyczolek = ("1", "Pomnik Lotnika", ["01"]), ("9", "Rembertów - Kolonia", ["01"]), "2100"
    gdecka = [("21", "Gdecka", ["01"]), ("22", "Łukowska", ["01"]), (przyczolek, "Przyczółek Grochowski", ["03"])]
    szembeka = [("31", "Pl. Szembeka", ["01"]), ("2008", "Wiatraczna", ["51"]), ("2008", "Wiatraczna", ["09"])]
    szembeka_once = [("31", "Pl. Szembeka", ["01"]), ("2008", "Wiatraczna", ["09", "51"])]
    tail = [(przyczolek, "Przyczółek Grochowski", ["01"]), ("40", "Saska", ["01"]), lotnika]
    outbound = [
        lotnika,
        ("2130", "Os. Majdańska", ["02"]),
        ("2008", "Wiatraczna", ["10"]),
        ("2008", "Wiatraczna", ["22"]),
        kolonia,
    ]
    outbound_short = [lotnika, ("2130", "Os. Majdańska", ["52"]), ("2008", "Wiatraczna", ["22"]), kolonia]
    courses = [
        _variant(1, 1727, outbound),
        _variant(0, 988, [kolonia, *gdecka, ("40", "Saska", ["01"]), lotnika]),
        _variant(0, 414, [kolonia, *szembeka, *tail]),
        _variant(0, 408, [kolonia, *szembeka_once, *tail]),
        _variant(1, 186, outbound_short),
        _variant(1, 70, [("50", "Płowiecka", ["01"]), ("2008", "Wiatraczna", ["22"]), kolonia]),
    ]

    columns = queries._route_columns(courses)  # noqa: SLF001

    assert [[(group["title"], group["title_note"]) for group in column] for column in columns] == [
        [("→ Rembertów - Kolonia", None), ("→ Rembertów - Kolonia", "from Płowiecka")],
        [("→ Pomnik Lotnika", None)],
    ]
    outbound_families, inbound_families = (column[0]["families"] for column in columns)
    assert [(family["label"], family["trip_count"]) for family in outbound_families] == [(None, 1913)]
    assert [(family["label"], family["trip_count"]) for family in inbound_families] == [
        ("Via Gdecka", 988),
        ("Via Pl. Szembeka", 822),
    ]
    assert [course["trip_count"] for course in (outbound_families[0]["main"], inbound_families[1]["main"])] == [
        1727,
        414,
    ]
    assert [course["deviation_label"] for course in outbound_families[0]["deviations"]] == [
        "One call at Wiatraczna [22]"
    ]
    assert [course["deviation_label"] for course in inbound_families[1]["deviations"]] == [
        "One call at Wiatraczna [09, 51]"
    ]
    assert inbound_families[0]["deviations"] == []


def test_route_card_lists_deviating_patterns_under_the_main_one(
    pattern_db: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with duckdb.connect(str(pattern_db)) as connection:
        connection.execute("""
            insert into mart_line_course_window
            select * replace ('loop-once' as route_pattern_id, 4 as stop_call_count, 1 as trip_count, 4 as course_rank)
            from mart_line_course_window
            where route_pattern_id = 'loop' and line = '1' and mode = 'bus' and window_type = 'month'
              and source_end_date = date '2026-06-30';
            insert into mart_line_course_stop_window
            select * replace ('loop-once' as route_pattern_id, call_position - (call_position > 3)::int as call_position)
            from mart_line_course_stop_window
            where route_pattern_id = 'loop' and call_position != 3 and line = '1' and mode = 'bus'
              and window_type = 'month' and source_end_date = date '2026-06-30';
        """)
    monkeypatch.setenv("ZTM_DUCKDB_PATH", str(pattern_db))
    monkeypatch.setattr(queries, "get_export_metadata", lambda _path: {})
    html = create_app().test_client().get("/lines/1?mode=bus&date=2026-06-30&window=month").get_data(as_text=True)

    assert "2 routes · 11 trips" in html
    assert "5 stops · 7 trips" in html
    assert "6 of 7 trips shown · other patterns:" in html
    assert re.search(r"One call at Wiatraczna \[03, 04\]</span>\s*<span[^>]*>4 stops · 1 trips", html)
