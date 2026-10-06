from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

import pytest

if TYPE_CHECKING:
    from pathlib import Path

from .test_dag_serving_export import (
    _load_dag_module,
    _load_validator_module,
    _test_export_config,
    _write_minimal_parquet_files,
)

# SQL identifiers and expressions below come only from fixed fixture parameters.
# ruff: noqa: S608


@pytest.fixture
def validator() -> Any:
    return _load_validator_module()


@pytest.fixture
def pattern_connection(tmp_path: Path) -> Any:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    paths = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    with duckdb.connect() as connection:
        for table_name, table_paths in paths.items():
            connection.execute(f"create table {table_name} as select * from read_parquet(?)", [str(table_paths[0])])
        connection.execute("create table export_metadata as select 'test' as export_id")
        connection.execute("""
            update mart_line_course_window set stop_call_count = 3;
            insert into mart_line_course_stop_window
            select * replace (2 as call_position, 2 as display_rank, ['03', '04'] as stop_post_codes)
            from mart_line_course_stop_window;
            insert into mart_line_course_stop_window
            select * replace (3 as call_position, 3 as display_rank,
                0 as arrival_count, null as mean_delay_seconds, null as median_delay_seconds,
                null as p90_delay_seconds, null as delay_spread_seconds,
                0 as early_count, 0 as on_time_count, 0 as late_count,
                null as early_rate, null as on_time_rate, null as late_rate, null as delay_histogram)
            from mart_line_course_stop_window where call_position = 1;
            insert into mart_line_course_window
            select * replace ('alternate' as route_pattern_id, 1 as stop_call_count, 2 as course_rank)
            from mart_line_course_window;
            insert into mart_line_course_stop_window
            select * replace ('alternate' as route_pattern_id)
            from mart_line_course_stop_window where call_position = 1;
            insert into mart_line_course_window
            select * replace ('unclassified' as route_pattern_id, 'unclassified' as pattern_status,
                null as origin_stop_name, null as destination_stop_name, null as stop_call_count, 3 as course_rank)
            from mart_line_course_window where route_pattern_id = 'pattern-1';
        """)
        yield connection


def test_accepts_patterns_with_repeated_groups_scheduled_calls_and_unclassified_trips(
    validator: Any, pattern_connection: Any
) -> None:
    validator._validate_required_columns(pattern_connection)
    validator._validate_route_pattern_ids(pattern_connection)
    validator._validate_route_patterns(pattern_connection, ("2026-07-02",))


@pytest.mark.parametrize(
    ("mutation", "contract"),
    [
        ("insert into mart_line_course_window select * from mart_line_course_window", "route_pattern_unique_key"),
        (
            "insert into mart_line_course_stop_window select * from mart_line_course_stop_window",
            "route_pattern_unique_key",
        ),
        ("delete from mart_line_course_stop_window where call_position = 2", "route_pattern_calls"),
        ("delete from mart_line_course_stop_window", "route_pattern_calls"),
        (
            "update mart_line_course_stop_window set call_position = 4, display_rank = 4 where call_position = 3",
            "route_pattern_calls",
        ),
        (
            "insert into mart_line_course_stop_window select * replace (4 as call_position, 4 as display_rank) from mart_line_course_stop_window where call_position = 3",
            "route_pattern_calls",
        ),
        (
            "update mart_line_course_stop_window set route_pattern_id = 'orphan' where route_pattern_id = 'pattern-1'",
            "route_pattern_calls",
        ),
        (
            "insert into mart_line_course_stop_window select * replace ('unclassified' as route_pattern_id) from mart_line_course_stop_window where route_pattern_id = 'alternate'",
            "route_pattern_calls",
        ),
        ("update mart_line_course_window set pattern_status = 'unknown'", "route_pattern_metadata"),
        ("update mart_line_course_window set pattern_status = null", "route_pattern_metadata"),
        (
            "update mart_line_course_window set pattern_status = 'unclassified' where route_pattern_id = 'pattern-1'",
            "route_pattern_metadata",
        ),
        (
            "update mart_line_course_window set origin_stop_name = 'Fabricated' where route_pattern_id = 'unclassified'",
            "route_pattern_metadata",
        ),
        (
            "update mart_line_course_window set stop_call_count = 1 where route_pattern_id = 'unclassified'",
            "route_pattern_metadata",
        ),
        (
            "update mart_line_course_window set stop_call_count = null where route_pattern_id = 'pattern-1'",
            "route_pattern_metadata",
        ),
        ("update mart_line_course_window set observed_service_dates = [null]", "route_pattern_metadata"),
        ("update mart_line_course_window set observed_service_dates = []", "route_pattern_metadata"),
        ("update mart_line_course_window set observed_service_dates = [date '2026-07-03']", "route_pattern_metadata"),
        (
            "update mart_line_course_stop_window set call_position = 0 where call_position = 3",
            "route_pattern_stop_contract",
        ),
        ("update mart_line_course_stop_window set display_rank = 99", "route_pattern_stop_contract"),
        ("update mart_line_course_stop_window set stop_post_codes = [null]", "route_pattern_stop_contract"),
        ("update mart_line_course_stop_window set stop_post_codes = ['01', '01']", "route_pattern_stop_contract"),
        ("update mart_line_course_stop_window set arrival_count = -1", "route_pattern_stop_contract"),
        (
            "update mart_line_course_stop_window set mean_delay_seconds = 0 where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set median_delay_seconds = 0 where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set p90_delay_seconds = 0 where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set on_time_rate = 0 where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set delay_histogram = [] where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set on_time_count = 1 where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
        (
            "update mart_line_course_stop_window set has_min_sample = true where arrival_count = 0",
            "route_pattern_stop_contract",
        ),
    ],
)
def test_rejects_material_pattern_corruption(
    validator: Any, pattern_connection: Any, mutation: str, contract: str
) -> None:
    pattern_connection.execute(mutation)
    with pytest.raises(validator.SemanticValidationError, match=contract):
        validator._validate_route_patterns(pattern_connection, ("2026-07-02",))


@pytest.mark.parametrize(
    "mutation",
    [
        "update mart_line_course_stop_window set arrival_count = 1, on_time_count = 1 where arrival_count = 0",
        "update mart_line_course_stop_window set on_time_count = 0 where arrival_count > 0",
        "update mart_line_course_stop_window set delay_histogram = [{'bucket_label': 'on_time_late_30_60s', 'n': 0}] where arrival_count > 0",
    ],
)
def test_unobserved_calls_cannot_be_counted_as_delay_samples(
    validator: Any, pattern_connection: Any, mutation: str
) -> None:
    pattern_connection.execute(mutation)
    with pytest.raises(validator.SemanticValidationError, match="route_pattern_stop_contract"):
        validator._validate_route_patterns(pattern_connection, ("2026-07-02",))


def test_nullable_direction_and_headsign_still_match_calls(validator: Any, pattern_connection: Any) -> None:
    for table in ("mart_line_course_window", "mart_line_course_stop_window"):
        pattern_connection.execute(f"update {table} set direction_id = null, trip_headsign = null")
    validator._validate_route_patterns(pattern_connection, ("2026-07-02",))


@pytest.mark.parametrize("partitioned_store", [False, True])
def test_pattern_failure_blocks_publication_and_preserves_previous_artifacts(
    tmp_path: Path, partitioned_store: bool
) -> None:
    duckdb = pytest.importorskip("duckdb")
    dag = _load_dag_module()
    paths = _write_minimal_parquet_files(tmp_path, dag.MART_TABLES, duckdb)
    call_path = paths["mart_line_course_stop_window"][0]
    replacement_path = call_path.with_name("invalid.parquet")
    with duckdb.connect() as connection:
        connection.read_parquet(str(call_path)).project("* replace (99 as display_rank)").write_parquet(
            str(replacement_path)
        )
    replacement_path.replace(call_path)
    stats = [dag.TableStats(table, 2 if table == "mart_pipeline_status" else 1, 10) for table in dag.MART_TABLES]
    config = replace(_test_export_config(dag, tmp_path), partitioned_store=partitioned_store)
    artifact = tmp_path / "ztm.duckdb"
    metadata = tmp_path / "ztm.duckdb.meta.json"
    artifact.write_bytes(b"old artifact")
    metadata.write_bytes(b"old metadata")
    with pytest.raises(RuntimeError, match="route_pattern_stop_contract"):
        dag._publish_duckdb(config, paths, stats, datetime(2026, 7, 2, tzinfo=UTC))
    assert artifact.read_bytes() == b"old artifact"
    assert metadata.read_bytes() == b"old metadata"


@pytest.mark.parametrize("field", ["mode", "line", "window_type", "window_key", "trip_headsign"])
def test_calls_must_match_the_full_pattern_group(validator: Any, pattern_connection: Any, field: str) -> None:
    pattern_connection.execute(f"update mart_line_course_stop_window set {field} = 'other'")
    with pytest.raises(validator.SemanticValidationError, match="route_pattern_calls"):
        validator._validate_route_patterns(pattern_connection, ("2026-07-02",))


@pytest.mark.parametrize("table", ["mart_line_course_window", "mart_line_course_stop_window"])
@pytest.mark.parametrize("pattern_id", [None, ""])
def test_rejects_legacy_ids_in_unchanged_retained_partitions(
    validator: Any, pattern_connection: Any, table: str, pattern_id: str | None
) -> None:
    pattern_connection.execute(
        f"insert into {table} select * replace (date '2026-06-01' as source_end_date, ?::varchar as route_pattern_id) from {table}",
        [pattern_id],
    )
    with pytest.raises(validator.SemanticValidationError, match="route_pattern_id:"):
        validator._validate_route_pattern_ids(pattern_connection)


def test_changed_pattern_date_is_checked_even_without_a_serving_date(validator: Any, pattern_connection: Any) -> None:
    pattern_connection.execute("""
        insert into mart_line_course_stop_window
        select * replace (date '2026-07-01' as source_end_date) from mart_line_course_stop_window;
    """)
    validator._validate_route_patterns(pattern_connection, ("2026-07-02",))
    with pytest.raises(validator.SemanticValidationError, match="route_pattern_calls"):
        validator._validate_route_patterns(pattern_connection, ("2026-07-01", "2026-07-02"))


@pytest.mark.parametrize(
    ("table", "column"),
    [
        ("mart_line_course_window", "route_pattern_id"),
        ("mart_line_course_window", "observed_service_dates"),
        ("mart_line_course_stop_window", "call_position"),
        ("mart_line_course_stop_window", "stop_post_codes"),
    ],
)
def test_route_pattern_columns_are_required(validator: Any, pattern_connection: Any, table: str, column: str) -> None:
    pattern_connection.execute(f"alter table {table} drop column {column}")
    with pytest.raises(validator.SemanticValidationError, match=f"required_columns:{table}"):
        validator._validate_required_columns(pattern_connection)


@pytest.mark.parametrize(
    ("table", "column", "replacement"),
    [
        ("mart_line_course_window", "route_pattern_id", "1"),
        ("mart_line_course_window", "observed_service_dates", "['2026-07-02']"),
        ("mart_line_course_stop_window", "stop_post_codes", "[1]"),
    ],
)
def test_rejects_wrong_pattern_column_types(
    validator: Any, pattern_connection: Any, table: str, column: str, replacement: str
) -> None:
    pattern_connection.execute(
        f"create or replace table {table} as select * replace ({replacement} as {column}) from {table}"
    )
    with pytest.raises(validator.SemanticValidationError, match="route_pattern_column_type:"):
        validator._validate_route_pattern_ids(pattern_connection)
