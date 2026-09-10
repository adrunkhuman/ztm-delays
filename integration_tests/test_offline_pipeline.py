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
from ztm_frontend.queries import get_trip_detail
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
) -> None:
    """Test-only dbt bypass: expose matcher facts without reproducing warehouse logic."""
    trip_facts = matcher_output / "reconstruction_trip_facts.parquet"
    expected_events = matcher_output / "reconstruction_expected_stop_events.parquet"
    with duckdb.connect() as connection:
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
                   and event.vehicle_number = trip.vehicle_number) as delay_profile
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


def test_tiny_fixture_reaches_frontend_through_real_offline_export(tmp_path: Path) -> None:
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
    _adapt_matcher_facts_for_serving(matcher_output, paths_by_table)
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
    assert exported_events == [(1, 60), (2, 60)]

    detail = get_trip_detail(db_path, "cross-midnight", SERVICE_DATE.isoformat(), "50")
    assert (detail["trip"]["trip_id"], detail["trip"]["start_delay_seconds"], len(detail["trip"]["trace"])) == (
        "cross-midnight",
        60,
        2,
    )
    assert [(row["stop_name"], row["delay_seconds"], row["observation_status"]) for row in detail["trip_stops"]] == [
        ("A", 60, "observed"),
        ("B", 60, "observed"),
    ]
