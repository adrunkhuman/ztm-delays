from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import pytest

from ztm_frontend import queries


def test_get_lines_keeps_same_line_bus_and_tram_separate(tmp_path: Path) -> None:
    """Same public line id can exist in both modes; selected mode must scope detail reads."""
    first_previously_hidden_stop_rank = 37
    db_path = tmp_path / "ztm.duckdb"
    _create_line_smoke_db(db_path)

    result = queries.get_lines(db_path, "1", "bus", "2026-06-30", None)

    assert result["summary"]["mode"] == "bus"
    assert {row["mode"] for row in result["line_list"]} == {"bus"}
    assert [course["trip_headsign"] for course in result["courses"]] == [
        "Bus destination",
        "Bus destination 2",
        "Bus destination 3",
    ]
    assert result["courses"][2]["stops"][0]["display_rank"] == first_previously_hidden_stop_rank
    assert result["line_widgets"]["worst"][0]["direction"] == "Bus destination"


def test_get_lines_reads_month_summary_and_hour_aggregates(tmp_path: Path) -> None:
    expected_month_delay = 90.0
    expected_month_trip_count = 40
    db_path = tmp_path / "ztm.duckdb"
    _create_line_smoke_db(db_path)

    result = queries.get_lines(db_path, "1", "bus", "2026-06-30", None, None, "month")

    assert result["selected_window"] == "month"
    assert result["summary"]["window_key"] == "2026-06"
    assert result["summary"]["median_delay_seconds"] == expected_month_delay
    assert result["line_widgets"]["hours"][4]["delay"] == expected_month_delay
    assert result["courses"][0]["trip_count"] == expected_month_trip_count
    assert result["courses"][0]["stops"][0]["median_delay_seconds"] == expected_month_delay


def test_get_export_metadata_ignores_stale_sidecar(tmp_path: Path) -> None:
    """A failed metadata write must not pair old sidecar status with a newer DuckDB file."""
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table export_metadata as
            select
                'new-export' as export_id,
                'alpha-1' as export_version,
                'current_pipeline_provisional' as source_mode,
                timestamp '2026-07-02 12:00:00' as exported_at,
                1::ubigint as source_row_count,
                10::ubigint as duckdb_file_size_bytes
            """
        )
    Path(f"{db_path}.meta.json").write_text(
        json.dumps({"export_id": "old-export", "poller_status": {"status": "ok"}}), encoding="utf-8"
    )

    metadata = queries.get_export_metadata(db_path)

    assert metadata["export_id"] == "new-export"
    assert "poller_status" not in metadata


def test_trip_stops_use_selected_trip_snapshot(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table fct_expected_stop_event as
            select * from (
                values
                    ('snapshot-a', date '2026-06-30', 'trip-1', '1001', 0, '100101', '1001', '01', 'Old stop', timestamp '2026-06-30 08:00:00', timestamp '2026-06-30 08:01:00', 60, 'observed'),
                    ('snapshot-b', date '2026-06-30', 'trip-1', '1001', 0, '100101', '1001', '01', 'Current stop', timestamp '2026-06-30 08:00:00', timestamp '2026-06-30 08:02:00', 120, 'observed'),
                    ('snapshot-b', date '2026-06-30', 'trip-1', '1001', 1, '999999', '9999', '99', 'Depot', timestamp '2026-06-30 08:05:00', null, null, 'not_in_passenger_service')
            ) as rows(
                gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence, stop_id,
                stop_group_id, stop_post_code, stop_name, scheduled_arrival_time, actual_arrival_time,
                delay_seconds, observation_status
            )
            """
        )

    rows = queries._trip_stops(db_path, "2026-06-30", "snapshot-b", "trip-1", "1001")  # noqa: SLF001

    assert [(row["stop_name"], row["delay_seconds"]) for row in rows] == [("Current stop", 120)]


def test_trip_landing_rows_are_paginated_before_traces_are_built(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_trip_daily as
            select
                date '2026-06-30' as service_date,
                'bus' as mode,
                'complete' as trip_quality,
                'weekday' as schedule_day_type,
                range::double as end_delay_seconds,
                range::double as erratic_score,
                range + 1 as landing_worst_rank,
                range + 1 as landing_best_rank,
                range + 1 as landing_erratic_rank,
                'trip-' || lpad((range + 1)::varchar, 2, '0') as trip_id,
                []::double[] as delay_profile
            from range(52)
            """
        )

    first_rows, first_page = queries._trip_landing_rows(  # noqa: SLF001
        db_path, "2026-06-30", "bus", "worst", 1
    )
    third_rows, third_page = queries._trip_landing_rows(  # noqa: SLF001
        db_path, "2026-06-30", "bus", "worst", 3
    )

    assert len(first_rows) == queries.LANDING_PAGE_SIZE
    assert first_rows[0]["trip_id"] == "trip-52"
    assert first_rows[-1]["trip_id"] == "trip-33"
    assert first_page == {"page": 1, "first_item": 1, "has_previous": False, "has_next": True}
    assert [row["trip_id"] for row in third_rows] == [f"trip-{number:02}" for number in range(12, 0, -1)]
    assert third_page == {"page": 3, "first_item": 41, "has_previous": True, "has_next": False}


def test_page_parameter_defaults_to_first_page() -> None:
    requested_page = 3

    assert queries._selected_page(None) == 1  # noqa: SLF001
    assert queries._selected_page("not-a-number") == 1  # noqa: SLF001
    assert queries._selected_page("-4") == 1  # noqa: SLF001
    assert queries._selected_page(str(requested_page)) == requested_page  # noqa: SLF001
    assert queries._selected_page(str(2**128)) == queries.MAX_PAGE  # noqa: SLF001


def test_line_rail_groups_regular_replacement_and_night_lines() -> None:
    rows = [
        {"line": "N36"},
        {"line": "Z21"},
        {"line": "E-1"},
        {"line": "733"},
        {"line": "102"},
        {"line": "Z-8"},
    ]

    groups = queries._line_rail_groups(rows)  # noqa: SLF001

    assert [[row["line"] for row in group] for group in groups] == [
        ["102", "733", "E-1"],
        ["Z-8", "Z21"],
        ["N36"],
    ]


def test_ranked_entities_apply_limit_and_offset(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_line_window_summary as
            select
                range::varchar as line,
                'bus' as mode,
                'zone1_public' as universe_type,
                'day' as window_type,
                '2026-06-30' as window_key,
                date '2026-06-30' as source_end_date
            from range(6);

            create table mart_entity_rankings as
            select
                range::varchar as entity_id,
                'bus' as mode,
                'line' as entity_type,
                'median_delay_seconds' as metric,
                'day' as window_type,
                '2026-06-30' as window_key,
                date '2026-06-30' as source_end_date,
                range + 1 as rank,
                6 as n_entities,
                range::double as value
            from range(6);
            """
        )

    rows = queries._ranked_entities(  # noqa: SLF001
        db_path, "2026-06-30", "line", "median_delay_seconds", "bus", limit=3, offset=2
    )

    assert [row["rank"] for row in rows] == [3, 4, 5]


def test_ranked_entity_limit_applies_per_mode(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_line_window_summary as
            select
                mode || range::varchar as line,
                mode,
                'zone1_public' as universe_type,
                'day' as window_type,
                '2026-06-30' as window_key,
                date '2026-06-30' as source_end_date
            from (values ('bus'), ('tram')) as modes(mode)
            cross join range(3);

            create table mart_entity_rankings as
            select
                mode || range::varchar as entity_id,
                mode,
                'line' as entity_type,
                'median_delay_seconds' as metric,
                'day' as window_type,
                '2026-06-30' as window_key,
                date '2026-06-30' as source_end_date,
                range + 1 as rank,
                3 as n_entities,
                range::double as value
            from (values ('bus'), ('tram')) as modes(mode)
            cross join range(3);
            """
        )

    rows = queries._ranked_entities(  # noqa: SLF001
        db_path, "2026-06-30", "line", "median_delay_seconds", limit=2
    )

    assert [(row["mode"], row["rank"]) for row in rows] == [
        ("bus", 1),
        ("bus", 2),
        ("tram", 1),
        ("tram", 2),
    ]


def test_selected_line_trip_rows_are_paginated(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_trip_daily as
            select
                date '2026-06-30' as service_date,
                'bus' as mode,
                '1' as line,
                'complete' as trip_quality,
                'weekday' as schedule_day_type,
                range + 1 as departure_rank,
                'trip-' || lpad((range + 1)::varchar, 2, '0') as trip_id
            from range(52)
            """
        )

    rows, pagination = queries._selected_line_trip_rows(  # noqa: SLF001
        db_path, "2026-06-30", "bus", "1", "departure_rank", 3
    )

    assert [row["trip_id"] for row in rows] == [f"trip-{number:02}" for number in range(41, 53)]
    assert pagination == {"page": 3, "first_item": 41, "has_previous": True, "has_next": False}


def test_weekday_trip_rows_use_matching_days_and_active_schedule_version(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_trip_daily as
            select * from (
                values
                    (date '2026-07-13', 'weekday', 'current', 'bus', 'complete', 120.0, 10.0, 'current-trip', []::double[]),
                    (date '2026-07-11', 'saturday', 'current', 'bus', 'complete', 999.0, 10.0, 'weekend-trip', []::double[]),
                    (date '2026-06-29', 'weekday', 'old', 'bus', 'complete', 888.0, 10.0, 'old-trip', []::double[])
            ) as rows(service_date, schedule_day_type, schedule_version_id, mode, trip_quality, end_delay_seconds, erratic_score, trip_id, delay_profile);

            create table dim_schedule_version as
            select * from (
                values
                    ('current', date '2026-07-01', null::date),
                    ('old', date '2026-01-01', date '2026-06-30')
            ) as rows(schedule_version_id, valid_from_date, valid_to_date);
            """
        )

    rows, _ = queries._trip_landing_rows(  # noqa: SLF001
        db_path,
        "2026-07-15",
        "bus",
        "worst",
        1,
        "weekdays",
        {"source_start_date": date(2026, 6, 1), "source_end_date": date(2026, 7, 15)},
    )

    assert [row["trip_id"] for row in rows] == ["current-trip"]


def test_stop_line_rows_include_records_after_old_top_30_cap(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_stop_line_window_summary as
            select
                'day' as window_type,
                '2026-06-30' as window_key,
                date '2026-06-30' as source_end_date,
                'bus' as mode,
                'stop_post' as entity_type,
                '100101' as entity_id,
                range + 1 as display_rank
            from range(52)
            """
        )

    first_rows, first_page = queries._stop_line_rows(  # noqa: SLF001
        db_path, "2026-06-30", "bus", "100101", 1
    )
    third_rows, third_page = queries._stop_line_rows(  # noqa: SLF001
        db_path, "2026-06-30", "bus", "100101", 3
    )

    assert [row["display_rank"] for row in first_rows][-1] == queries.LANDING_PAGE_SIZE
    assert first_page["has_next"] is True
    assert [row["display_rank"] for row in third_rows] == list(range(41, 53))
    assert third_page["has_next"] is False


def test_stop_auxiliary_rows_use_selected_window_and_anchor(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_stop_post_line_group_window as
            select * from (
                values
                    ('month', '2026-07', date '2026-07-15', 'bus', '1001', '100101', 'Current', []::struct(line varchar, mode varchar, route_short_name varchar)[], 1),
                    ('month', '2026-07', date '2026-07-14', 'bus', '1001', '100101', 'Stale', []::struct(line varchar, mode varchar, route_short_name varchar)[], 1)
            ) as rows(window_type, window_key, source_end_date, mode, stop_group_id, stop_id, trip_headsign, lines, display_rank);

            create table mart_stop_group_line_group_window as
            select * from (
                values
                    ('month', '2026-07', date '2026-07-15', 'bus', '1001', '1', '1', 'Current', []::struct(stop_id varchar, stop_post_code varchar)[], 1, 1),
                    ('month', '2026-07', date '2026-07-14', 'bus', '1001', '1', '1', 'Stale', []::struct(stop_id varchar, stop_post_code varchar)[], 1, 1)
            ) as rows(window_type, window_key, source_end_date, mode, stop_group_id, line, route_short_name, trip_headsign, posts, line_display_rank, destination_display_rank);
            """
        )

    post_groups = queries._line_groups_by_post(  # noqa: SLF001
        db_path, "2026-07", "2026-07-15", "bus", "1001", "month"
    )
    line_groups = queries._stop_group_line_groups(  # noqa: SLF001
        db_path, "2026-07", "2026-07-15", "bus", "1001", "month"
    )

    assert post_groups["100101"][0]["trip_headsign"] == "Current"
    assert line_groups[0]["destinations"][0]["trip_headsign"] == "Current"


def test_page_result_supports_compact_picker_pages() -> None:
    rows = [{"id": index} for index in range(queries.STOP_PICKER_PAGE_SIZE + 1)]

    page_rows, pagination = queries._page_result(  # noqa: SLF001
        rows, 2, page_size=queries.STOP_PICKER_PAGE_SIZE
    )

    assert len(page_rows) == queries.STOP_PICKER_PAGE_SIZE
    assert pagination == {"page": 2, "first_item": 13, "has_previous": True, "has_next": True}


def test_week_bars_cover_selected_calendar_week(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_entity_daily_summary as
            select * from (
                values
                    ('line', '1', 'bus', date '2026-07-13', 10.0),
                    ('line', '1', 'bus', date '2026-07-14', 20.0),
                    ('line', '1', 'bus', date '2026-07-19', 70.0),
                    ('line', '1', 'bus', date '2026-07-20', 80.0)
            ) as rows(entity_type, entity_id, mode, service_date, median_delay_seconds)
            """
        )

    bars = queries._week_bars(db_path, "line", "1", "bus", "2026-07-13")  # noqa: SLF001

    assert [row["service_date"] for row in bars] == [
        "2026-07-13",
        "2026-07-14",
        "2026-07-15",
        "2026-07-16",
        "2026-07-17",
        "2026-07-18",
        "2026-07-19",
    ]
    assert [row["label"] for row in bars] == ["M", "T", "W", "T", "F", "S", "S"]
    assert [row["delay"] for row in bars] == [10.0, 20.0, None, None, None, None, 70.0]
    assert [row["selected"] for row in bars] == [True, False, False, False, False, False, False]


def test_weekday_comparison_excludes_weekend_service(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_entity_window_daily_summary as
            select * from (
                values
                    ('line', '1', 'bus', 'weekdays', '2026-07-13', date '2026-07-13', date '2026-07-06', 20.0),
                    ('line', '1', 'bus', 'weekdays', '2026-07-13', date '2026-07-13', date '2026-07-07', 40.0),
                    ('line', '1', 'bus', 'weekend', '2026-07-13', date '2026-07-13', date '2026-07-11', 999.0),
                    ('line', '1', 'bus', 'weekdays', '2026-07-13', date '2026-07-13', date '2026-07-13', 60.0)
            ) as rows(entity_type, entity_id, mode, window_type, window_key, source_end_date, service_date, median_delay_seconds)
            """
        )

    bars = queries._comparison_bars(  # noqa: SLF001
        db_path,
        "line",
        "1",
        "bus",
        "2026-07-13",
        "weekdays",
    )

    assert [row["delay"] for row in bars] == [20.0, 40.0, 60.0]


def test_window_context_describes_matching_service_dates(tmp_path: Path) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)):
        pass
    context = queries._window_context(  # noqa: SLF001
        db_path,
        "weekend",
        "2026-07-15",
        {
            "source_start_date": date(2026, 1, 3),
            "source_end_date": date(2026, 7, 13),
            "source_day_count": 60,
        },
    )

    assert context["label"] == "03 Jan–13 Jul · 60d"  # noqa: RUF001


def _create_line_smoke_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as connection:
        _execute_many(
            connection,
            [
                "create table dim_serving_date as select '2026-06-30' as service_date_key",
                """
                create table dim_serving_window_date as
                select 'day' as window_type, '2026-06-30' as window_key, date '2026-06-30' as source_end_date,
                    date '2026-06-30' as service_date
                union all
                select 'month', '2026-06', date '2026-06-30', date '2026-06-30'
                """,
                """
                create table dim_schedule_version as
                select 'current' as schedule_version_id, date '2026-01-01' as valid_from_date,
                    null::date as valid_to_date
                """,
                _line_window_summary_sql(),
                _line_course_window_sql(),
                _line_course_stop_window_sql(),
                _worst_delay_event_sql(),
                _line_reliability_sql(),
                _hour_window_summary_sql(),
                _entity_daily_summary_sql(),
                _entity_window_daily_summary_sql(),
                _entity_timeline_daily_sql(),
            ],
        )


def _execute_many(connection: duckdb.DuckDBPyConnection, statements: list[str]) -> None:
    for statement in statements:
        connection.execute(statement)


def _line_window_summary_sql() -> str:
    return """
        create table mart_line_window_summary as
        select * from (
            values
                ('1', 'bus', '1', 'Bus route', 'all_observed', 'day', '2026-06-30', date '2026-06-30', 1, 10, 20.0, 30.0, 60.0, 90.0, 1, 8, 1, 0.1, 0.8, 0.1, []),
                ('1', 'tram', '1', 'Tram route', 'all_observed', 'day', '2026-06-30', date '2026-06-30', 1, 10, 200.0, 220.0, 300.0, 100.0, 0, 2, 8, 0.0, 0.2, 0.8, []),
                ('1', 'bus', '1', 'Bus route', 'all_observed', 'month', '2026-06', date '2026-06-30', 20, 200, 80.0, 90.0, 180.0, 90.0, 10, 150, 40, 0.05, 0.75, 0.2, []),
                ('1', 'bus', '1', 'Stale route', 'all_observed', 'month', '2026-06', date '2026-06-29', 19, 190, 900.0, 999.0, 1200.0, 201.0, 0, 0, 190, 0.0, 0.0, 1.0, [])
        ) as rows(
            line, mode, route_short_name, route_label, universe_type, window_type, window_key, source_end_date, trip_count,
            arrival_count, mean_delay_seconds, median_delay_seconds, p90_delay_seconds, delay_spread_seconds,
            early_count, on_time_count, late_count, early_rate, on_time_rate, late_rate, delay_histogram
        )
    """


def _line_course_window_sql() -> str:
    return """
        create table mart_line_course_window as
        select * from (
            values
                ('1', 'bus', 0, 'Bus destination', 'day', '2026-06-30', date '2026-06-30', 4, 1),
                ('1', 'bus', 1, 'Bus destination 2', 'day', '2026-06-30', date '2026-06-30', 3, 2),
                ('1', 'bus', 2, 'Bus destination 3', 'day', '2026-06-30', date '2026-06-30', 2, 3),
                ('1', 'tram', 0, 'Tram destination', 'day', '2026-06-30', date '2026-06-30', 4, 1),
                ('1', 'bus', 0, 'Bus destination', 'month', '2026-06', date '2026-06-30', 40, 1)
        ) as rows(line, mode, direction_id, trip_headsign, window_type, window_key, source_end_date, trip_count, course_rank)
    """


def _line_course_stop_window_sql() -> str:
    return """
        create table mart_line_course_stop_window as
        select * from (
            values
                ('1', 'bus', 0, 'Bus destination', '7002', '700201', '01', 'Bus Stop', 'day', '2026-06-30', date '2026-06-30', 1, 4, 20.0, 30.0, 60.0, 30.0, 0, 4, 0, 0.0, 1.0, 0.0, [], true),
                ('1', 'bus', 2, 'Bus destination 3', '7003', '700301', '01', 'Bus Stop 37', 'day', '2026-06-30', date '2026-06-30', 37, 1, 20.0, 30.0, 60.0, 30.0, 0, 1, 0, 0.0, 1.0, 0.0, [], false),
                ('1', 'tram', 0, 'Tram destination', '8002', '800201', '01', 'Tram Stop', 'day', '2026-06-30', date '2026-06-30', 1, 4, 200.0, 220.0, 300.0, 80.0, 0, 1, 3, 0.0, 0.25, 0.75, [], true),
                ('1', 'bus', 0, 'Bus destination', '7002', '700201', '01', 'Bus Stop', 'month', '2026-06', date '2026-06-30', 1, 40, 80.0, 90.0, 180.0, 90.0, 2, 30, 8, 0.05, 0.75, 0.2, [], true)
        ) as rows(
            line, mode, direction_id, trip_headsign, stop_group_id, stop_id, stop_post_code, stop_name, window_type,
            window_key, source_end_date, display_rank, arrival_count, mean_delay_seconds, median_delay_seconds, p90_delay_seconds,
            delay_spread_seconds, early_count, on_time_count, late_count, early_rate, on_time_rate, late_rate,
            delay_histogram, has_min_sample
        )
    """


def _worst_delay_event_sql() -> str:
    return """
        create table mart_worst_delay_event as
        select *, 'trip-1' as trip_id, '1001' as vehicle_number, '1' as line from (
            values
                (date '2026-06-30', 'weekday', timestamp '2026-06-30 08:00:00', 'bus', 'line', '1', 1, '08:00', 'Bus destination', 'Bus Stop', '7002', 60.0),
                (date '2026-06-30', 'weekday', timestamp '2026-06-30 08:00:00', 'tram', 'line', '1', 1, '08:00', 'Tram destination', 'Tram Stop', '8002', 300.0)
        ) as rows(service_date, schedule_day_type, scheduled_arrival_time, mode, scope_type, scope_id, delay_rank, time_label, trip_headsign, stop_name, stop_group_id, delay_seconds)
    """


def _line_reliability_sql() -> str:
    return """
        create table mart_line_reliability_daily as
        select * from (
            values
                (date '2026-06-30', 'weekday', 'bus', '1', 0, 'Bus destination', 4, 0, 0, [], 1),
                (date '2026-06-30', 'weekday', 'tram', '1', 0, 'Tram destination', 1, 1, 2, [], 1)
        ) as rows(service_date, schedule_day_type, mode, line, direction_id, trip_headsign, clean_count, partial_count, broken_count, outcomes, display_rank)
    """


def _hour_window_summary_sql() -> str:
    return """
        create table mart_hour_window_summary as
        select 'line' as entity_type, '1' as entity_id, 'bus' as mode, 'day' as window_type, '2026-06-30' as window_key,
            date '2026-06-30' as source_end_date, 8 as local_hour, 4 as service_hour_index,
            30.0 as median_delay_seconds, true as has_min_sample
        union all
        select 'line', '1', 'bus', 'month', '2026-06', date '2026-06-30', 8, 4, 90.0, true
        union all
        select 'line', '1', 'bus', 'month', '2026-06', date '2026-06-29', 8, 4, 999.0, true
    """


def _entity_daily_summary_sql() -> str:
    return """
        create table mart_entity_daily_summary as
        select 'line' as entity_type, '1' as entity_id, 'bus' as mode, date '2026-06-30' as service_date,
            'weekday' as schedule_day_type, 30.0 as median_delay_seconds
    """


def _entity_window_daily_summary_sql() -> str:
    return """
        create table mart_entity_window_daily_summary as
        select 'line' as entity_type, '1' as entity_id, 'bus' as mode, 'month' as window_type,
            '2026-06' as window_key, date '2026-06-30' as source_end_date, date '2026-06-30' as service_date,
            90.0 as median_delay_seconds
    """


def _entity_timeline_daily_sql() -> str:
    return """
        create table mart_entity_timeline_daily as
        select date '2026-06-30' as service_date, 'line' as entity_type, '1' as entity_id, 'bus' as mode,
            50.0 as x_percent, 30.0 as delay_seconds, 1 as point_rank
    """


@pytest.mark.parametrize("window", ["weekdays", "weekend", "month"])
@pytest.mark.parametrize(
    ("entity_type", "entity_id", "mode"), [("mode", "bus", None), ("line", "1", "bus"), ("stop_post", "100102", "bus")]
)
def test_grouped_charts_keep_exact_daily_values(
    tmp_path: Path, window: str, entity_type: str, entity_id: str, mode: str | None
) -> None:
    db_path = tmp_path / "ztm.duckdb"
    key = "2026-07" if window == "month" else "2026-07-31"
    day_count = 31 if window == "month" else 60
    with duckdb.connect(str(db_path)) as connection:
        connection.execute(
            """
            create table mart_entity_window_daily_summary as
            select ? as entity_type, ? as entity_id, 'bus' as mode, ? as window_type,
                   ? as window_key, date '2026-07-31' as source_end_date,
                   date '2026-07-31' - range::integer as service_date,
                   case when range = 0 then null when range = 1 then -120
                        when range = 2 then 0 else range * 10 end as median_delay_seconds
            from range(?)
            """,
            [entity_type, entity_id, window, key, day_count],
        )
        connection.execute(
            """
            insert into mart_entity_window_daily_summary
            select entity_type, entity_id, 'tram', window_type, window_key,
                   source_end_date, service_date, 9999 from mart_entity_window_daily_summary;
            insert into mart_entity_window_daily_summary
            select entity_type, entity_id, mode, window_type, window_key,
                   date '2026-07-30', service_date, 9999 from mart_entity_window_daily_summary
            """
        )
    # Mode entities are already identified by entity_id; use the fixture's mode
    # predicate here to also verify mode isolation at every display grain.
    selected_mode = mode or "bus"
    recent = queries._comparison_bars(db_path, entity_type, entity_id, selected_mode, "2026-07-31", window)  # noqa: SLF001
    daily = queries._period_daily_bars(db_path, entity_type, entity_id, selected_mode, "2026-07-31", window)  # noqa: SLF001
    assert [row["delay"] for row in recent] == [*range(110, 20, -10), 0, -120, None]
    assert len(daily) == day_count
    assert [row["service_date"] for row in recent] == [row["service_date"] for row in daily[-12:]]
    assert [row["tone"] for row in recent[-3:]] == ["zero", "early", "missing"]
    assert recent[-1]["height"] == 0
    max_bar_height = 45
    assert all(0 <= row["height"] <= max_bar_height for row in daily)
    assert sum(bool(row["label"]) for row in daily) < len(daily)
    assert "median" in queries._comparison_label(window)  # noqa: SLF001


@pytest.mark.parametrize(
    ("window", "label"),
    [
        ("day", "07 Sep 2026"),
        ("month", "Sep 2026"),
        ("weekdays", "29 Jun–07 Sep · 49d"),  # noqa: RUF001
        ("weekend", "29 Jun–07 Sep · 49d"),  # noqa: RUF001
    ],
)
def test_compact_period_labels(tmp_path: Path, window: str, label: str) -> None:
    db_path = tmp_path / "ztm.duckdb"
    with duckdb.connect(str(db_path)):
        pass
    context = queries._window_context(  # noqa: SLF001
        db_path,
        window,
        "2026-09-07",
        {"source_start_date": date(2026, 6, 29), "source_end_date": date(2026, 9, 7), "source_day_count": 49},
    )
    assert context["label"] == label
    if window != "day":
        assert queries._comparison_label(window) == "12 service days · median"  # noqa: SLF001
