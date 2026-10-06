from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from types import ModuleType
from typing import Any

import duckdb
import pyarrow.parquet as pq
import pytest
from ztm_frontend.queries import get_lines, get_trip_detail
from ztm_matcher import ReconstructionRun, RunConfig

ROOT = Path(__file__).parents[1]
SERVICE_DATE = date(2026, 1, 14)
PROCESSING_DATE = date(2026, 1, 15)


def _load_test_helpers(module_name: str, path: Path) -> ModuleType:
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"failed to load test helpers from {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _run_matcher(tmp_path: Path, matcher_helpers: ModuleType) -> tuple[Path, dict[str, Any]]:
    gps_root = tmp_path / "gps"
    gtfs_zip = tmp_path / "snapshot.zip"
    output = tmp_path / "matcher-output"
    matcher_helpers._cross_midnight_gtfs(gtfs_zip)

    # One minute of lateness makes matcher provenance visible in the final UI rows.
    start = datetime(2026, 1, 14, 22, 51, tzinfo=UTC)
    pings = []
    for minute in range(0, 31, 2):
        gps_time = start + timedelta(minutes=minute)
        longitude = 21.0 if minute == 0 else 21.01 if minute == 30 else 21.005
        pings.append(
            matcher_helpers._row(
                Lines="n50",
                Brigade="0050",
                VehicleNumber="50",
                Lat=52.2,
                Lon=longitude,
                Time=gps_time,
                ingested_at=gps_time,
            )
        )
    matcher_helpers._gps_for_date(gps_root, date(2026, 1, 14), pings[:5])
    matcher_helpers._gps_for_date(gps_root, date(2026, 1, 15), pings[5:])

    config = RunConfig(
        PROCESSING_DATE,
        "synthetic",
        gps_root,
        gtfs_zip,
        output,
        output / "metrics.json",
        memory_limit="256MB",
        temp_limit="64MB",
        max_vehicle_rows=100,
        allow_missing_hours=True,
    )
    with ReconstructionRun(config) as run:
        result = run.prepare()
    return output, result


def _copy_query(connection: Any, path: Path, query: str, parameters: list[object]) -> None:
    path.unlink(missing_ok=True)
    escaped_path = path.as_posix().replace("'", "''")
    connection.execute(f"copy ({query}) to '{escaped_path}' (format parquet)", parameters)


def _adapt_matcher_facts_for_serving(
    matcher_output: Path,
    paths_by_table: dict[str, list[Path]],
    *,
    unobserved_last_call: bool = False,
) -> None:
    """Test-only dbt bypass: expose matcher facts without reproducing warehouse logic."""
    trip_facts = matcher_output / "reconstruction_trip_facts.parquet"
    expected_events = matcher_output / "reconstruction_expected_stop_events.parquet"
    with duckdb.connect() as connection:
        if unobserved_last_call:
            # Add a missed scheduled-call scenario without changing the matcher output under test.
            adapted_events = matcher_output / "serving-expected-events.parquet"
            _copy_query(
                connection,
                adapted_events,
                """
                select * replace (
                    case when stop_sequence = 2 then null else actual_arrival_time end as actual_arrival_time,
                    case when stop_sequence = 2 then null else delay_seconds end as delay_seconds,
                    case when stop_sequence = 2 then 'missed' else observation_status end as observation_status
                ) from read_parquet(?)
                """,
                [str(expected_events)],
            )
            expected_events = adapted_events
        _copy_query(
            connection,
            paths_by_table["mart_trip_daily"][0],
            """
            select
                trip.*,
                trip.line as route_short_name,
                0 as direction_id,
                'Night' as trip_headsign,
                (select list(delay_seconds order by stop_sequence)
                 from read_parquet(?) as event
                 where event.gtfs_snapshot_id = trip.gtfs_snapshot_id
                   and event.service_date = trip.service_date
                   and event.trip_id = trip.trip_id
                   and event.vehicle_number = trip.vehicle_number
                   and event.observation_status = 'observed'
                   and event.delay_seconds is not null) as delay_profile
            from read_parquet(?) as trip
            """,
            [str(expected_events), str(trip_facts)],
        )
        _copy_query(
            connection,
            paths_by_table["fct_expected_stop_event"][0],
            """
            select
                event.*,
                right(event.stop_id, 2) as stop_post_code,
                case event.stop_id when '100001' then 'A' when '100002' then 'B' end as stop_name
            from read_parquet(?) as event
            """,
            [str(expected_events)],
        )
        _copy_query(
            connection,
            paths_by_table["dim_serving_date"][0],
            """
            select distinct
                service_date,
                cast(service_date as varchar) as service_date_key,
                null::date as previous_service_date,
                null::date as next_service_date,
                true as is_latest,
                1 as service_date_rank_desc
            from read_parquet(?)
            """,
            [str(trip_facts)],
        )
        _copy_query(
            connection,
            paths_by_table["mart_mode_window_summary"][0],
            """
            select distinct
                mode,
                'day' as window_type,
                cast(service_date as varchar) as window_key,
                service_date as source_start_date,
                service_date as source_end_date,
                1 as source_day_count
            from read_parquet(?)
            """,
            [str(trip_facts)],
        )
        _adapt_route_pattern_marts(connection, paths_by_table)
        status_path = paths_by_table["mart_pipeline_status"][0]
        connection.execute("create temp table fixture_status as select * from read_parquet(?)", [str(status_path)])
        _copy_query(
            connection,
            status_path,
            """
            select * replace (
                date '2026-01-14' as service_date,
                'synthetic' as latest_gtfs_snapshot_id,
                timestamp '2026-01-15 00:00:00' as latest_gtfs_snapshot_at,
                timestamp '2026-01-15 00:00:00' as status_generated_at
            )
            from fixture_status
            """,
            [],
        )


def _adapt_route_pattern_marts(connection: Any, paths_by_table: dict[str, list[Path]]) -> None:
    """Expose the tiny known itinerary, not a second implementation of dbt classification."""
    connection.execute(
        "create temp table serving_trips as select * from read_parquet(?) where trip_quality = 'complete'",
        [str(paths_by_table["mart_trip_daily"][0])],
    )
    connection.execute(
        "create temp table serving_events as select * replace "
        "(case when observation_status = 'observed' then delay_seconds end as delay_seconds) from read_parquet(?)",
        [str(paths_by_table["fct_expected_stop_event"][0])],
    )
    connection.execute("""
        create temp table fixture_course as
        select line, mode, route_short_name, direction_id, trip_headsign,
            'all_observed' as universe_type, 'day' as window_type,
            service_date::varchar as window_key, service_date as source_end_date,
            'synthetic-A-B' as route_pattern_id, 'classified' as pattern_status,
            'A' as origin_stop_name, 'B' as destination_stop_name, 2 as stop_call_count,
            [service_date] as observed_service_dates, count(*) as trip_count, 1 as course_rank
        from serving_trips group by all;
        create temp table fixture_calls as
        select course.line, course.mode, course.route_short_name, course.direction_id, course.trip_headsign,
            course.universe_type, course.window_type, course.window_key, course.source_end_date,
            course.route_pattern_id, event.stop_sequence as call_position, event.stop_sequence as display_rank,
            event.stop_sequence, event.stop_group_id, event.stop_id,
            event.stop_post_code, [event.stop_post_code] as stop_post_codes, event.stop_name,
            count(event.delay_seconds) as arrival_count,
            avg(event.delay_seconds) as mean_delay_seconds, median(event.delay_seconds) as median_delay_seconds,
            max(event.delay_seconds) as p90_delay_seconds,
            max(event.delay_seconds) - median(event.delay_seconds) as delay_spread_seconds,
            0 as early_count, count(event.delay_seconds) as on_time_count, 0 as late_count,
            case when count(event.delay_seconds) > 0 then 0.0 end as early_rate,
            case when count(event.delay_seconds) > 0 then 1.0 end as on_time_rate,
            case when count(event.delay_seconds) > 0 then 0.0 end as late_rate,
            case when count(event.delay_seconds) > 0 then
                [{'bucket_label': 'on_time_late_30_60s', 'n': count(event.delay_seconds)}] end as delay_histogram,
            count(event.delay_seconds) >= 3 as has_min_sample
        from fixture_course as course
        join serving_trips as trip on course.source_end_date = trip.service_date
            and course.line = trip.line and course.mode = trip.mode
        join serving_events as event using (gtfs_snapshot_id, service_date, trip_id, vehicle_number)
        group by all;
    """)
    for table_name, query in {
        "mart_line_course_window": "select * from fixture_course",
        "mart_line_course_stop_window": "select * from fixture_calls",
        # Keep the unrelated fixture line summary; this adapter covers route cards, not line widgets.
        "mart_line_window_summary": """
            select '190' as line, course.mode, '190' as route_short_name, course.universe_type,
                course.window_type, course.window_key, course.source_end_date as source_start_date,
                course.source_end_date, 1 as source_day_count, course.trip_count,
                (select sum(arrival_count) from fixture_calls) as arrival_count
            from fixture_course as course
        """,
        "dim_serving_window_date": """
            select distinct window_type, window_key, source_end_date, source_end_date as service_date
            from fixture_course
        """,
    }.items():
        _copy_query(connection, paths_by_table[table_name][0], query, [])


def _source_stats(dag: ModuleType, paths_by_table: dict[str, list[Path]]) -> list[Any]:
    stats = []
    with duckdb.connect() as connection:
        for table_name in dag.MART_TABLES:
            paths = paths_by_table[table_name]
            row_count = sum(
                connection.execute("select count(*) from read_parquet(?)", [str(path)]).fetchone()[0] for path in paths
            )
            stats.append(dag.TableStats(table_name, row_count, sum(path.stat().st_size for path in paths)))
    return stats


@pytest.mark.parametrize("unobserved_last_call", [False, True])
def test_tiny_fixture_reaches_frontend_through_real_offline_export(tmp_path: Path, unobserved_last_call: bool) -> None:
    """Cover matcher/export/frontend offline; dbt is intentionally replaced by a tiny row adapter."""
    matcher_helpers = _load_test_helpers("matcher_runtime_helpers", ROOT / "matcher/tests/test_runtime.py")
    export_helpers = _load_test_helpers("serving_export_helpers", ROOT / "airflow/tests/test_dag_serving_export.py")
    matcher_output, matcher_result = _run_matcher(tmp_path, matcher_helpers)

    matcher_trips = pq.read_table(matcher_output / "reconstruction_trip_facts.parquet").to_pylist()
    matcher_events = pq.read_table(matcher_output / "reconstruction_expected_stop_events.parquet").to_pylist()
    assert matcher_result["metrics"]["input_rows"] == 16
    assert [(row["trip_id"], row["trip_quality"], row["start_delay_seconds"]) for row in matcher_trips] == [
        ("cross-midnight", "complete", 60)
    ]
    assert [(row["stop_sequence"], row["delay_seconds"], row["source_gps_date"]) for row in matcher_events] == [
        (1, 60, date(2026, 1, 14)),
        (2, 60, date(2026, 1, 15)),
    ]

    dag = export_helpers._load_dag_module()
    paths_by_table = export_helpers._write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    _adapt_matcher_facts_for_serving(matcher_output, paths_by_table, unobserved_last_call=unobserved_last_call)
    stats = _source_stats(dag, paths_by_table)
    config = replace(
        export_helpers._test_export_config(dag, tmp_path / "serving"),
        changed_partition_dates=(SERVICE_DATE.isoformat(),),
        validation_timeout_seconds=30,
        validation_memory_limit_mb=512,
        validation_temp_limit_mb=64,
    )
    export = dag._publish_duckdb(config, paths_by_table, stats, datetime(2026, 1, 15, tzinfo=UTC))

    db_path = Path(export.duckdb_path)
    with duckdb.connect(str(db_path), read_only=True) as connection:
        metadata = connection.execute(
            "select exported_table_count, semantic_validation_status from export_metadata"
        ).fetchone()
        exported_events = connection.execute(
            "select stop_sequence, delay_seconds from fct_expected_stop_event order by stop_sequence"
        ).fetchall()
    assert metadata == (28, "pass")
    last_delay = None if unobserved_last_call else 60
    assert exported_events == [(1, 60), (2, last_delay)]

    line_detail = get_lines(db_path, "n50", "bus", SERVICE_DATE.isoformat(), None)
    assert len(line_detail["courses"]) == 1
    course = line_detail["courses"][0]
    assert (course["route_pattern_id"], course["pattern_status"], course["stop_call_count"]) == (
        "synthetic-A-B",
        "classified",
        2,
    )
    assert (course["origin_stop_name"], course["destination_stop_name"]) == ("A", "B")
    assert [(row["call_position"], row["display_rank"], row["stop_post_codes"]) for row in course["stops"]] == [
        (1, 1, ["01"]),
        (2, 2, ["02"]),
    ]
    # These calls share a stop group; the frontend must retain both positions.
    assert [row["stop_group_id"] for row in course["stops"]] == ["1000", "1000"]
    assert [row["arrival_count"] for row in course["stops"]] == [1, 0 if unobserved_last_call else 1]
    if unobserved_last_call:
        last_call = course["stops"][1]
        assert all(
            last_call[field] is None
            for field in (
                "mean_delay_seconds",
                "median_delay_seconds",
                "p90_delay_seconds",
                "on_time_rate",
                "delay_histogram",
                "shape",
            )
        )
        assert last_call["has_min_sample"] is False

    detail = get_trip_detail(db_path, "cross-midnight", SERVICE_DATE.isoformat(), "50")
    assert (detail["trip"]["trip_id"], detail["trip"]["start_delay_seconds"], len(detail["trip"]["trace"])) == (
        "cross-midnight",
        60,
        1 if unobserved_last_call else 2,
    )
    assert [(row["stop_name"], row["delay_seconds"], row["observation_status"]) for row in detail["trip_stops"]] == [
        ("A", 60, "observed"),
        ("B", last_delay, "missed" if unobserved_last_call else "observed"),
    ]
