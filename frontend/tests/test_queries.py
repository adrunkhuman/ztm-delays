from __future__ import annotations

import json
from pathlib import Path

import duckdb

from ztm_frontend import queries


def test_get_lines_keeps_same_line_bus_and_tram_separate(tmp_path: Path) -> None:
    """Same public line id can exist in both modes; selected mode must scope detail reads."""
    db_path = tmp_path / "ztm.duckdb"
    _create_line_smoke_db(db_path)

    result = queries.get_lines(db_path, "1", "bus", "2026-06-30", None)

    assert result["summary"]["mode"] == "bus"
    assert {row["mode"] for row in result["line_list"]} == {"bus"}
    assert [course["trip_headsign"] for course in result["courses"]] == ["Bus destination"]
    assert result["line_widgets"]["worst"][0]["direction"] == "Bus destination"


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
                    ('snapshot-b', date '2026-06-30', 'trip-1', '1001', 0, '100101', '1001', '01', 'Current stop', timestamp '2026-06-30 08:00:00', timestamp '2026-06-30 08:02:00', 120, 'observed')
            ) as rows(
                gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence, stop_id,
                stop_group_id, stop_post_code, stop_name, scheduled_arrival_time, actual_arrival_time,
                delay_seconds, observation_status
            )
            """
        )

    rows = queries._trip_stops(db_path, "2026-06-30", "snapshot-b", "trip-1", "1001")  # noqa: SLF001

    assert [(row["stop_name"], row["delay_seconds"]) for row in rows] == [("Current stop", 120)]


def _create_line_smoke_db(db_path: Path) -> None:
    with duckdb.connect(str(db_path)) as connection:
        _execute_many(
            connection,
            [
                "create table dim_serving_date as select '2026-06-30' as service_date_key",
                _line_window_summary_sql(),
                _line_course_window_sql(),
                _line_course_stop_window_sql(),
                _worst_delay_event_sql(),
                _line_reliability_sql(),
                _hour_window_summary_sql(),
                _entity_daily_summary_sql(),
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
                ('1', 'bus', '1', 'Bus route', 'all_observed', 'day', '2026-06-30', 1, 10, 20.0, 30.0, 60.0, 90.0, 1, 8, 1, 0.1, 0.8, 0.1, []),
                ('1', 'tram', '1', 'Tram route', 'all_observed', 'day', '2026-06-30', 1, 10, 200.0, 220.0, 300.0, 100.0, 0, 2, 8, 0.0, 0.2, 0.8, [])
        ) as rows(
            line, mode, route_short_name, route_label, universe_type, window_type, window_key, trip_count,
            arrival_count, mean_delay_seconds, median_delay_seconds, p90_delay_seconds, delay_spread_seconds,
            early_count, on_time_count, late_count, early_rate, on_time_rate, late_rate, delay_histogram
        )
    """


def _line_course_window_sql() -> str:
    return """
        create table mart_line_course_window as
        select * from (
            values
                ('1', 'bus', 0, 'Bus destination', 'day', '2026-06-30', 4, 1),
                ('1', 'tram', 0, 'Tram destination', 'day', '2026-06-30', 4, 1)
        ) as rows(line, mode, direction_id, trip_headsign, window_type, window_key, trip_count, course_rank)
    """


def _line_course_stop_window_sql() -> str:
    return """
        create table mart_line_course_stop_window as
        select * from (
            values
                ('1', 'bus', 0, 'Bus destination', '7002', '700201', '01', 'Bus Stop', 'day', '2026-06-30', 1, 4, 20.0, 30.0, 60.0, 30.0, 0, 4, 0, 0.0, 1.0, 0.0, [], true),
                ('1', 'tram', 0, 'Tram destination', '8002', '800201', '01', 'Tram Stop', 'day', '2026-06-30', 1, 4, 200.0, 220.0, 300.0, 80.0, 0, 1, 3, 0.0, 0.25, 0.75, [], true)
        ) as rows(
            line, mode, direction_id, trip_headsign, stop_group_id, stop_id, stop_post_code, stop_name, window_type,
            window_key, display_rank, arrival_count, mean_delay_seconds, median_delay_seconds, p90_delay_seconds,
            delay_spread_seconds, early_count, on_time_count, late_count, early_rate, on_time_rate, late_rate,
            delay_histogram, has_min_sample
        )
    """


def _worst_delay_event_sql() -> str:
    return """
        create table mart_worst_delay_event as
        select * from (
            values
                (date '2026-06-30', 'bus', 'line', '1', 1, '08:00', 'Bus destination', 'Bus Stop', '7002', 60.0),
                (date '2026-06-30', 'tram', 'line', '1', 1, '08:00', 'Tram destination', 'Tram Stop', '8002', 300.0)
        ) as rows(service_date, mode, scope_type, scope_id, delay_rank, time_label, trip_headsign, stop_name, stop_group_id, delay_seconds)
    """


def _line_reliability_sql() -> str:
    return """
        create table mart_line_reliability_daily as
        select * from (
            values
                (date '2026-06-30', 'bus', '1', 0, 'Bus destination', 4, 0, 0, [], 1),
                (date '2026-06-30', 'tram', '1', 0, 'Tram destination', 1, 1, 2, [], 1)
        ) as rows(service_date, mode, line, direction_id, trip_headsign, clean_count, partial_count, broken_count, outcomes, display_rank)
    """


def _hour_window_summary_sql() -> str:
    return """
        create table mart_hour_window_summary as
        select 'line' as entity_type, '1' as entity_id, 'bus' as mode, 'day' as window_type, '2026-06-30' as window_key,
            8 as local_hour, 4 as service_hour_index, 30.0 as median_delay_seconds, true as has_min_sample
    """


def _entity_daily_summary_sql() -> str:
    return """
        create table mart_entity_daily_summary as
        select 'line' as entity_type, '1' as entity_id, 'bus' as mode, date '2026-06-30' as service_date,
            30.0 as median_delay_seconds
    """


def _entity_timeline_daily_sql() -> str:
    return """
        create table mart_entity_timeline_daily as
        select date '2026-06-30' as service_date, 'line' as entity_type, '1' as entity_id, 'bus' as mode,
            50.0 as x_percent, 30.0 as delay_seconds, 1 as point_rank
    """
