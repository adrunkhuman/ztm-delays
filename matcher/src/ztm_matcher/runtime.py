"""Bounded DuckDB runtime and grouped vehicle seam."""

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Iterator
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass
from datetime import date
from itertools import groupby
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ztm_matcher.alignment import extract_evidence, resolve_competing_ownership, settle_duty
from ztm_matcher.config import RunConfig
from ztm_matcher.errors import fail
from ztm_matcher.facts import build_facts
from ztm_matcher.gps import discover, hourly_counts, normalize
from ztm_matcher.gtfs import Snapshot, load, select
from ztm_matcher.schemas import (
    DUTY_EXECUTION_SCHEMA,
    EXECUTION_SCHEMA_VERSION,
    EXPECTED_STOP_EVENT_SCHEMA_VERSION,
    MANIFEST_VERSION,
    NORMALIZED_GPS_SCHEMA,
    NORMALIZED_GPS_SCHEMA_VERSION,
    OPERATIONAL_CROSSING_SCHEMA_VERSION,
    PASSENGER_ARRIVAL_SCHEMA_VERSION,
    PASSENGER_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
    RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
    RECONSTRUCTION_TRIP_FACT_SCHEMA,
    SCHEDULE_SCHEMA_VERSION,
    SEMANTICS_SCHEMA_VERSION,
    STOP_ARRIVAL_FACT_SCHEMA_VERSION,
    STOP_CROSSING_SCHEMA,
    STOP_SEMANTICS_SCHEMA,
    TRAVERSAL_EVIDENCE_SCHEMA,
    TRIP_FACT_SCHEMA_VERSION,
    TRIP_UNIVERSE_SCHEMA,
    TRIP_UNIVERSE_SCHEMA_VERSION,
)
from ztm_matcher.semantics import duties, iter_stop_semantics
from ztm_matcher.stop_alignment import align_stop_crossings

SEMANTICS_BATCH_ROWS = 10_000
STOP_ALIGNMENT_ROW_GROUP_ROWS = 25_000
TRIP_UNIVERSE_BATCH_ROWS = 25_000
STOP_ALIGNMENT_SEMANTIC_COLUMNS = (
    "stop_id",
    "stop_group_id",
    "stop_lat",
    "stop_lon",
    "stop_sequence",
    "arrival_time_seconds",
    "departure_time_seconds",
    "pickup_type",
    "drop_off_type",
    "stop_service_class",
    "stop_execution_class",
    "classification_confidence",
    "classification_reason",
    "classification_evidence",
    "is_passenger_stop",
    "are_passenger_boundaries_settled",
    "first_passenger_stop_sequence",
    "last_passenger_stop_sequence",
)
STOP_ALIGNMENT_EXECUTION_KEY = (
    "service_date",
    "processing_date",
    "gtfs_snapshot_id",
    "duty_chain_id",
    "trip_order",
    "trip_id",
    "vehicle_number",
)


@dataclass(frozen=True)
class VehicleStream:
    """One bounded, deterministically ordered vehicle GPS group."""

    vehicle_number: str
    pings: pa.Table


@dataclass(frozen=True)
class StopAlignmentWorker:
    """Pickle-safe description of a deterministic contiguous vehicle chunk."""

    index: int
    vehicle_numbers: tuple[str, ...]
    normalized_path: Path
    inputs_path: Path
    shard_dir: Path
    memory_limit: str
    temp_limit: str
    max_vehicle_rows: int


class OrderedArrowRows:
    """Incrementally consume rows ordered by vehicle_number."""

    def __init__(self, reader: pa.RecordBatchReader) -> None:
        self._rows = (row for batch in reader for row in batch.to_pylist())
        self.current = next(self._rows, None)

    def pop(self) -> dict[str, Any]:
        if self.current is None:
            raise StopIteration
        row = self.current
        self.current = next(self._rows, None)
        return row


def _write_trip_universe_group(
    group: list[dict[str, Any]], output_rows: list[dict[str, Any]], writer: pq.ParquetWriter
) -> None:
    """Classify one line-direction group without retaining the full trip universe."""
    candidates_by_length: dict[int, dict[tuple[str, ...], list[dict[str, Any]]]] = {}
    for row in group:
        if not row["is_public_passenger_segment"] or row["terminal_pair_rank"] <= 1:
            continue
        stop_ids = tuple(row["stop_ids"])
        candidates_by_length.setdefault(len(stop_ids), {}).setdefault(stop_ids, []).append(row)

    short_turn_parts: set[int] = set()
    for longer_trip in group:
        if not longer_trip["is_public_passenger_segment"]:
            continue
        longer_stop_ids = tuple(longer_trip["stop_ids"])
        for length, candidates in candidates_by_length.items():
            if length > len(longer_stop_ids):
                continue
            for start in range(len(longer_stop_ids) - length + 1):
                for candidate in candidates.get(longer_stop_ids[start : start + length], []):
                    if longer_trip["stop_count"] > candidate["stop_count"]:
                        short_turn_parts.add(id(candidate))

    for row in group:
        is_short_turn_part_trip = id(row) in short_turn_parts
        row["is_short_turn_part_trip"] = is_short_turn_part_trip
        row["is_zone1_only"] = row["non_zone1_stop_count"] == 0
        row["is_zone1_public_ranking_trip"] = (
            row["is_public_passenger_segment"] and not is_short_turn_part_trip and row["is_zone1_only"]
        )
        output_rows.append({name: row[name] for name in TRIP_UNIVERSE_SCHEMA.names})
        if len(output_rows) >= TRIP_UNIVERSE_BATCH_ROWS:
            writer.write_table(pa.Table.from_pylist(output_rows, schema=TRIP_UNIVERSE_SCHEMA))
            output_rows.clear()


def _rank_trip_universe_terminal_pairs(group: list[dict[str, Any]]) -> None:
    """Set deterministic terminal-pair counts and ranks for one line-direction group."""
    counts: dict[tuple[str, str], int] = {}
    for row in group:
        if row["is_public_passenger_segment"]:
            pair = (row["origin_stop_id"], row["destination_stop_id"])
            counts[pair] = counts.get(pair, 0) + 1
    ranks = {
        pair: rank
        for rank, (pair, _) in enumerate(
            sorted(counts.items(), key=lambda item: (-item[1], item[0][0], item[0][1])), start=1
        )
    }
    for row in group:
        pair = (row["origin_stop_id"], row["destination_stop_id"])
        row["terminal_pair_trip_count"] = counts.get(pair, 0)
        row["terminal_pair_rank"] = ranks.get(pair, 999999)


def _trip_universe_base_rows(
    duty_rows: list[dict[str, Any]], snapshot: Snapshot, settled_passenger_trips: set[tuple[date, str]]
) -> list[dict[str, Any]]:
    """Build compact per-course ranking inputs from the already loaded schedule snapshot."""
    base_rows = []
    for duty_row in duty_rows:
        if duty_row["mode"] not in {"bus", "tram"}:
            continue
        stop_ids = tuple(
            stop_time.stop_id
            for stop_time in sorted(snapshot.stop_times.get(duty_row["trip_id"], []), key=lambda row: row.stop_sequence)
        )
        non_zone1_stop_count = sum(
            snapshot.stops.get(stop_id) is None or snapshot.stops[stop_id].effective_zone_id != "1"
            for stop_id in stop_ids
        )
        is_public_passenger_segment = (
            duty_row["is_public_service_segment"] is True
            and duty_row["is_malformed_duty_segment"] is False
            and (duty_row["service_date"], duty_row["trip_id"]) in settled_passenger_trips
        )
        base_rows.append(
            {
                "gtfs_snapshot_id": duty_row["gtfs_snapshot_id"],
                "processing_date": duty_row["processing_date"],
                "service_date": duty_row["service_date"],
                "duty_chain_id": duty_row["duty_chain_id"],
                "trip_id": duty_row["trip_id"],
                "line": duty_row["line"],
                "mode": duty_row["mode"],
                "direction_id": duty_row["direction_id"],
                "origin_stop_id": duty_row["origin_stop_id"],
                "destination_stop_id": duty_row["destination_stop_id"],
                "ordered_stop_ids": "|".join(stop_ids),
                "stop_count": len(stop_ids),
                "non_zone1_stop_count": non_zone1_stop_count,
                "is_public_service_segment": duty_row["is_public_service_segment"],
                "is_public_passenger_segment": is_public_passenger_segment,
                "terminal_pair_trip_count": 0,
                "terminal_pair_rank": 999999,
                "is_short_turn_part_trip": False,
                "is_zone1_only": False,
                "is_zone1_public_ranking_trip": False,
                "stop_ids": stop_ids,
            }
        )
    return base_rows


def _balanced_vehicle_chunks(vehicle_numbers: list[str], worker_count: int) -> list[tuple[str, ...]]:
    """Split sorted vehicles into contiguous chunks that differ by at most one vehicle."""
    active_workers = min(worker_count, len(vehicle_numbers))
    if active_workers == 0:
        return []
    quotient, remainder = divmod(len(vehicle_numbers), active_workers)
    chunks = []
    start = 0
    for index in range(active_workers):
        stop = start + quotient + (index < remainder)
        chunks.append(tuple(vehicle_numbers[start:stop]))
        start = stop
    return chunks


def _stop_alignment_counts() -> dict[str, int]:
    return {
        "operational_stop_crossings": 0,
        "passenger_stop_arrivals": 0,
        "missing_stops": 0,
        "ambiguous_trips": 0,
        "vehicle_groups": 0,
        "execution_trips": 0,
    }


def _write_stop_alignment(
    execution: dict[str, Any] | None,
    semantics: list[dict[str, Any]],
    pings: list[dict[str, Any]],
    operational_rows: list[dict[str, Any]],
    passenger_rows: list[dict[str, Any]],
    counts: dict[str, int],
) -> None:
    """Align one course and append its rows to bounded worker-local buffers."""
    if execution is None:
        return
    lower = max(
        execution["ownership_interval_start_time"],
        execution["source_ping_start_time"] or execution["ownership_interval_start_time"],
    )
    upper = min(
        execution["ownership_interval_end_time"],
        execution["source_ping_end_time"] or execution["ownership_interval_end_time"],
    )
    result = align_stop_crossings(execution, semantics, [ping for ping in pings if lower <= ping["gps_time"] <= upper])
    operational_rows.extend(result.operational_crossings)
    passenger_rows.extend(result.passenger_arrivals)
    counts["operational_stop_crossings"] += len(result.operational_crossings)
    counts["passenger_stop_arrivals"] += len(result.passenger_arrivals)
    counts["missing_stops"] += result.missing_stop_count
    counts["ambiguous_trips"] += int(result.ambiguous)
    counts["execution_trips"] += 1


def _flush_stop_alignment_rows(
    rows: list[dict[str, Any]], writer: pq.ParquetWriter, schema: pa.Schema, *, final: bool = False
) -> None:
    """Write full deterministic row groups and retain at most one partial group."""
    while len(rows) >= STOP_ALIGNMENT_ROW_GROUP_ROWS:
        batch = rows[:STOP_ALIGNMENT_ROW_GROUP_ROWS]
        writer.write_table(pa.Table.from_pylist(batch, schema=schema))
        del rows[:STOP_ALIGNMENT_ROW_GROUP_ROWS]
    if final and rows:
        writer.write_table(pa.Table.from_pylist(rows, schema=schema))
        rows.clear()


def _cleanup_stop_alignment_worker(
    operational_rows: list[dict[str, Any]],
    passenger_rows: list[dict[str, Any]],
    operational_writer: pq.ParquetWriter | None,
    passenger_writer: pq.ParquetWriter | None,
    connections: tuple[duckdb.DuckDBPyConnection | None, ...],
) -> None:
    """Flush and close every resource, reporting only the first cleanup failure."""
    cleanup_error: BaseException | None = None

    def clean_up(action: Any) -> None:
        nonlocal cleanup_error
        try:
            action()
        except BaseException as error:
            if cleanup_error is None:
                cleanup_error = error

    if operational_writer is not None:
        clean_up(
            lambda: _flush_stop_alignment_rows(operational_rows, operational_writer, STOP_CROSSING_SCHEMA, final=True)
        )
    if passenger_writer is not None:
        clean_up(
            lambda: _flush_stop_alignment_rows(
                passenger_rows, passenger_writer, PASSENGER_STOP_ARRIVAL_SCHEMA, final=True
            )
        )
    if operational_writer is not None:
        clean_up(operational_writer.close)
    if passenger_writer is not None:
        clean_up(passenger_writer.close)
    for connection in connections:
        if connection is not None:
            clean_up(connection.close)
    if cleanup_error is not None:
        raise cleanup_error


def _pings_for_stop_alignment_vehicle(
    rows: OrderedArrowRows, vehicle_number: str, max_vehicle_rows: int
) -> list[dict[str, Any]]:
    """Consume one vehicle's bounded GPS group from the ordered chunk reader."""
    if rows.current is not None and str(rows.current["vehicle_number"]) < vehicle_number:
        raise RuntimeError("normalized GPS rows are not ordered by worker vehicle")
    pings: list[dict[str, Any]] = []
    while rows.current is not None and str(rows.current["vehicle_number"]) == vehicle_number:
        if len(pings) >= max_vehicle_rows:
            raise fail("resource_limit", f"vehicle {vehicle_number} exceeds max_vehicle_rows", 14)
        pings.append(rows.pop())
    return pings


def _write_stop_alignment_vehicle(
    rows: OrderedArrowRows,
    vehicle_number: str,
    pings: list[dict[str, Any]],
    operational_rows: list[dict[str, Any]],
    passenger_rows: list[dict[str, Any]],
    counts: dict[str, int],
    operational_writer: pq.ParquetWriter,
    passenger_writer: pq.ParquetWriter,
) -> None:
    """Consume and align one vehicle's executions without retaining the chunk's inputs."""
    if rows.current is not None and str(rows.current["vehicle_number"]) < vehicle_number:
        raise RuntimeError("stop alignment inputs are not ordered by worker vehicle")
    execution_key: tuple[object, ...] | None = None
    execution: dict[str, Any] | None = None
    semantics: list[dict[str, Any]] = []
    while rows.current is not None and str(rows.current["vehicle_number"]) == vehicle_number:
        row = rows.pop()
        row_key = tuple(row[name] for name in STOP_ALIGNMENT_EXECUTION_KEY)
        if execution_key is not None and row_key != execution_key:
            _write_stop_alignment(execution, semantics, pings, operational_rows, passenger_rows, counts)
            _flush_stop_alignment_rows(operational_rows, operational_writer, STOP_CROSSING_SCHEMA)
            _flush_stop_alignment_rows(passenger_rows, passenger_writer, PASSENGER_STOP_ARRIVAL_SCHEMA)
            execution = None
            semantics = []
        if execution is None:
            execution_key = row_key
            execution = {name: row[name] for name in DUTY_EXECUTION_SCHEMA.names}
        semantics.append({name: row[f"semantic_{name}"] for name in STOP_ALIGNMENT_SEMANTIC_COLUMNS})
    if execution is not None:
        _write_stop_alignment(execution, semantics, pings, operational_rows, passenger_rows, counts)
        _flush_stop_alignment_rows(operational_rows, operational_writer, STOP_CROSSING_SCHEMA)
        _flush_stop_alignment_rows(passenger_rows, passenger_writer, PASSENGER_STOP_ARRIVAL_SCHEMA)


def _run_stop_alignment_worker(worker: StopAlignmentWorker) -> dict[str, int]:
    """Align a chunk with an isolated, single-threaded DuckDB connection."""
    worker.shard_dir.mkdir(parents=True, exist_ok=True)
    temp_dir = worker.shard_dir / f"duckdb-{worker.index:03d}"
    normalized_temp_dir = temp_dir / "normalized"
    inputs_temp_dir = temp_dir / "inputs"
    normalized_temp_dir.mkdir(parents=True)
    inputs_temp_dir.mkdir()
    operational_path = worker.shard_dir / f"{worker.index:03d}-operational.parquet"
    passenger_path = worker.shard_dir / f"{worker.index:03d}-passenger.parquet"
    normalized_connection: duckdb.DuckDBPyConnection | None = None
    inputs_connection: duckdb.DuckDBPyConnection | None = None
    operational_writer: pq.ParquetWriter | None = None
    passenger_writer: pq.ParquetWriter | None = None
    operational_rows: list[dict[str, Any]] = []
    passenger_rows: list[dict[str, Any]] = []
    counts = _stop_alignment_counts()
    primary_error: BaseException | None = None
    normalized_sql = str(worker.normalized_path).replace("'", "''")
    inputs_sql = str(worker.inputs_path).replace("'", "''")
    try:
        normalized_connection = duckdb.connect()
        inputs_connection = duckdb.connect()
        for connection, connection_temp_dir in (
            (normalized_connection, normalized_temp_dir),
            (inputs_connection, inputs_temp_dir),
        ):
            connection.execute("set threads to 1")
            connection.execute(f"set memory_limit to '{worker.memory_limit}'")
            connection.execute(f"set max_temp_directory_size to '{worker.temp_limit}'")
            connection.execute(f"set temp_directory to '{str(connection_temp_dir).replace("'", "''")}'")
        operational_writer = pq.ParquetWriter(operational_path, STOP_CROSSING_SCHEMA, compression="zstd")
        passenger_writer = pq.ParquetWriter(passenger_path, PASSENGER_STOP_ARRIVAL_SCHEMA, compression="zstd")
        normalized_rows = OrderedArrowRows(
            normalized_connection.execute(
                f"""
                select * from read_parquet('{normalized_sql}')
                where vehicle_number in (select unnest(?))
                order by vehicle_number, gps_time, ingested_at, line, brigade
                """,
                [list(worker.vehicle_numbers)],
            ).to_arrow_reader(SEMANTICS_BATCH_ROWS)
        )
        input_rows = OrderedArrowRows(
            inputs_connection.execute(
                f"""
                select * from read_parquet('{inputs_sql}')
                where vehicle_number in (select unnest(?))
                order by vehicle_number, ownership_interval_start_time, service_date, duty_chain_id,
                    trip_order, trip_id, semantic_stop_sequence
                """,
                [list(worker.vehicle_numbers)],
            ).to_arrow_reader(SEMANTICS_BATCH_ROWS)
        )
        for vehicle_number in worker.vehicle_numbers:
            pings = _pings_for_stop_alignment_vehicle(normalized_rows, vehicle_number, worker.max_vehicle_rows)
            counts["vehicle_groups"] += 1
            _write_stop_alignment_vehicle(
                input_rows,
                vehicle_number,
                pings,
                operational_rows,
                passenger_rows,
                counts,
                operational_writer,
                passenger_writer,
            )
    except BaseException as error:
        primary_error = error
        raise
    finally:
        try:
            _cleanup_stop_alignment_worker(
                operational_rows,
                passenger_rows,
                operational_writer,
                passenger_writer,
                (normalized_connection, inputs_connection),
            )
        except BaseException:
            if primary_error is None:
                raise
    return counts


def _merge_stop_alignment_shards(
    shard_dir: Path, worker_count: int, output_path: Path, schema: pa.Schema, artifact_name: str
) -> None:
    """Concatenate contiguous vehicle shards in chunk order into one ordered artifact."""
    writer = pq.ParquetWriter(output_path, schema, compression="zstd")
    buffered: pa.Table | None = None
    try:
        for index in range(worker_count):
            shard = shard_dir / f"{index:03d}-{artifact_name}.parquet"
            for batch in pq.ParquetFile(shard).iter_batches(batch_size=STOP_ALIGNMENT_ROW_GROUP_ROWS):
                table = pa.Table.from_batches([batch], schema=schema)
                buffered = table if buffered is None else pa.concat_tables([buffered, table])
                while buffered.num_rows >= STOP_ALIGNMENT_ROW_GROUP_ROWS:
                    writer.write_table(buffered.slice(0, STOP_ALIGNMENT_ROW_GROUP_ROWS))
                    buffered = buffered.slice(STOP_ALIGNMENT_ROW_GROUP_ROWS)
        if buffered is not None and buffered.num_rows:
            writer.write_table(buffered)
    finally:
        writer.close()


def _identity(path: Path, *, relative: bool = False) -> dict[str, object]:
    """Return stable lineage; finalized artifacts must not expose a random staging path."""
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while chunk := source.read(1024 * 1024):
            digest.update(chunk)
    return {
        "path": path.name if relative else str(path),
        "bytes": path.stat().st_size,
        "sha256": digest.hexdigest(),
    }


def _process_measurements() -> dict[str, int | None]:
    """Capture best-effort process memory and current swap without a runtime dependency."""
    peak = None
    swap = None
    status = Path("/proc/self/status")
    if os.name != "nt" and status.is_file():
        for line in status.read_text(encoding="utf-8").splitlines():
            if line.startswith("VmHWM:"):
                peak = int(line.split()[1]) * 1024
            if line.startswith("VmSwap:"):
                swap = int(line.split()[1]) * 1024
    return {"peak_rss_bytes": peak, "current_swap_bytes": swap}


def _write_json(path: Path, value: dict[str, Any]) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class ReconstructionRun:
    """Prepare a single Warsaw day without materializing a full GPS day in Python."""

    def __init__(self, config: RunConfig) -> None:
        self.config, self.connection, self.work_dir, self.normalized_path = config, None, None, None

    def __enter__(self) -> "ReconstructionRun":
        if self.config.alignment_workers < 1:
            raise fail("invalid_configuration", "alignment_workers must be positive", 2)
        self.config.output_dir.parent.mkdir(parents=True, exist_ok=True)
        self.work_dir = self.config.output_dir.parent / f".{self.config.output_dir.name}.incomplete-{uuid.uuid4().hex}"
        self.work_dir.mkdir()
        temp = self.work_dir / "duckdb-temp"
        temp.mkdir()
        try:
            self.connection = duckdb.connect()
            self.connection.execute(f"set threads to {self.config.threads}")
            self.connection.execute(f"set memory_limit to '{self.config.memory_limit}'")
            self.connection.execute(f"set max_temp_directory_size to '{self.config.temp_limit}'")
            self.connection.execute(f"set temp_directory to '{str(temp).replace("'", "''")}'")
        except duckdb.Error as exc:
            raise fail("resource_limit", "unable to configure DuckDB resource limits", 14) from exc
        return self

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> None:
        if self.connection:
            self.connection.close()
        if exc_type is None:
            self._publish()

    def prepare_schedule(self) -> int:
        """Load pinned schedule, duty chains, and passenger-stop semantics."""
        work = self._work()
        snapshot = load(self.config.gtfs_zip, self.config.snapshot_id, self.config.processing_date)
        selected = select(snapshot, self.config.processing_date)
        if not selected:
            raise fail("invalid_output", "pinned snapshot has no trips overlapping the processing date", 15)
        rows = duties(selected, snapshot)
        schedule_table = pa.Table.from_pylist(rows)
        required_schedule = {
            "service_date",
            "processing_date",
            "gtfs_snapshot_id",
            "duty_chain_id",
            "duty_chain_source",
            "trip_order",
            "trip_id",
        }
        if not required_schedule.issubset(schedule_table.column_names):
            raise fail("invalid_output", "duty schedule does not satisfy schedule-v1", 15)
        pq.write_table(schedule_table, work / "duty_schedule.parquet", compression="zstd")
        semantics_path = work / "stop_semantics.parquet"
        semantics_batch: list[dict[str, Any]] = []
        semantics_writer: pq.ParquetWriter | None = None
        semantics_columns: set[str] | None = None
        semantics_count = 0
        settled_passenger_trips: set[tuple[date, str]] = set()
        for semantic in iter_stop_semantics(rows, snapshot):
            if (
                semantic["are_passenger_boundaries_settled"]
                and semantic["is_passenger_stop"]
                and semantic["stop_execution_class"] == "passenger"
            ):
                settled_passenger_trips.add((semantic["service_date"], semantic["trip_id"]))
            semantics_batch.append(semantic)
            if len(semantics_batch) < SEMANTICS_BATCH_ROWS:
                continue
            table = pa.Table.from_pylist(semantics_batch, schema=STOP_SEMANTICS_SCHEMA)
            semantics_columns = set(table.column_names)
            semantics_writer = semantics_writer or pq.ParquetWriter(semantics_path, table.schema, compression="zstd")
            semantics_writer.write_table(table)
            semantics_count += table.num_rows
            semantics_batch.clear()
        if semantics_batch:
            table = pa.Table.from_pylist(semantics_batch, schema=STOP_SEMANTICS_SCHEMA)
            semantics_columns = set(table.column_names)
            semantics_writer = semantics_writer or pq.ParquetWriter(semantics_path, table.schema, compression="zstd")
            semantics_writer.write_table(table)
            semantics_count += table.num_rows
        if semantics_writer is not None:
            semantics_writer.close()
        if semantics_count == 0:
            raise fail("invalid_output", "pinned snapshot produced no stop semantics", 15)
        required_semantics = {
            "service_date",
            "processing_date",
            "gtfs_snapshot_id",
            "trip_id",
            "stop_sequence",
            "stop_execution_class",
            "classification_evidence",
        }
        if semantics_columns is None or not required_semantics.issubset(semantics_columns):
            raise fail("invalid_output", "stop semantics do not satisfy stop-semantics-v1", 15)
        if pq.read_schema(semantics_path) != STOP_SEMANTICS_SCHEMA:
            raise fail("invalid_output", "stop semantics schema validation failed", 15)
        self._write_trip_universe(rows, snapshot, settled_passenger_trips, work / "trip_universe.parquet")
        schedule_sql = str(work / "duty_schedule.parquet").replace("'", "''")
        semantics_sql = str(semantics_path).replace("'", "''")
        terminal_sql = str(work / ".terminal_courses.parquet").replace("'", "''")
        self._connection().execute(
            f"""
            copy (
                with course_stops as (
                    select schedule.*, semantics.stop_id, semantics.stop_sequence,
                        semantics.stop_lat, semantics.stop_lon, semantics.is_passenger_stop,
                        semantics.are_passenger_boundaries_settled,
                        case when semantics.are_passenger_boundaries_settled
                            then semantics.is_passenger_stop else true end as endpoint_eligible
                    from read_parquet('{schedule_sql}') as schedule
                    inner join read_parquet('{semantics_sql}') as semantics
                        on schedule.service_date = semantics.service_date
                        and schedule.processing_date = semantics.processing_date
                        and schedule.gtfs_snapshot_id = semantics.gtfs_snapshot_id
                        and schedule.duty_chain_id = semantics.duty_chain_id
                        and schedule.trip_id = semantics.trip_id
                )
                select * exclude (stop_id, stop_sequence, stop_lat, stop_lon, is_passenger_stop,
                        are_passenger_boundaries_settled, endpoint_eligible),
                    bool_and(are_passenger_boundaries_settled) as are_passenger_boundaries_settled,
                    arg_min(stop_lat, stop_sequence)
                        filter (where endpoint_eligible and stop_lat is not null) as origin_lat,
                    arg_min(stop_lon, stop_sequence)
                        filter (where endpoint_eligible and stop_lon is not null) as origin_lon,
                    arg_max(stop_lat, stop_sequence)
                        filter (where endpoint_eligible and stop_lat is not null) as destination_lat,
                    arg_max(stop_lon, stop_sequence)
                        filter (where endpoint_eligible and stop_lon is not null) as destination_lon
                from course_stops
                group by all
                order by service_date, duty_chain_id, trip_order, trip_id
            ) to '{terminal_sql}' (format parquet, compression zstd)
            """
        )
        return len(rows)

    def _write_trip_universe(
        self,
        duty_rows: list[dict[str, Any]],
        snapshot: Snapshot,
        settled_passenger_trips: set[tuple[date, str]],
        output: Path,
    ) -> None:
        """Persist the schedule-derived local analogue of the serving ranking universe."""
        temporary_output = output.with_name(f".{output.name}.{uuid.uuid4().hex}.tmp")
        writer: pq.ParquetWriter | None = None
        try:
            base_rows = _trip_universe_base_rows(duty_rows, snapshot, settled_passenger_trips)
            base_rows.sort(
                key=lambda row: (
                    row["gtfs_snapshot_id"],
                    row["service_date"],
                    row["line"],
                    row["direction_id"],
                    row["trip_id"],
                    row["duty_chain_id"],
                )
            )
            writer = pq.ParquetWriter(temporary_output, TRIP_UNIVERSE_SCHEMA, compression="zstd")
            output_rows: list[dict[str, Any]] = []
            for _, group_rows in groupby(
                base_rows,
                key=lambda row: (row["gtfs_snapshot_id"], row["service_date"], row["line"], row["direction_id"]),
            ):
                group = list(group_rows)
                _rank_trip_universe_terminal_pairs(group)
                _write_trip_universe_group(group, output_rows, writer)
            if output_rows:
                writer.write_table(pa.Table.from_pylist(output_rows, schema=TRIP_UNIVERSE_SCHEMA))
            writer.close()
            writer = None
            temporary_output.replace(output)
        finally:
            try:
                if writer is not None:
                    writer.close()
            finally:
                temporary_output.unlink(missing_ok=True)
        if pq.read_schema(output) != TRIP_UNIVERSE_SCHEMA:
            raise fail("invalid_output", "trip universe schema validation failed", 15)

    def prepare(self) -> dict[str, Any]:
        """Produce validated artifacts plus deterministic lineage and resource metrics."""
        started = time.perf_counter()
        cpu_started = time.process_time()
        work, connection = self._work(), self._connection()
        files, missing = discover(self.config.gps_root, self.config.processing_date)
        schedule_rows = self.prepare_schedule()
        self.normalized_path = work / "normalized_gps.parquet"
        normalized_rows = normalize(connection, files, self.config.processing_date, self.normalized_path)
        execution_counts = self.align_execution()
        crossing_counts = self.align_stops()
        fact_counts = self.build_facts()
        input_rows = sum(pq.ParquetFile(file).metadata.num_rows for file in files)
        if pq.read_schema(self.normalized_path) != NORMALIZED_GPS_SCHEMA:
            raise fail("invalid_output", "normalized GPS artifact schema validation failed", 15)
        manifest = {
            "manifest_version": MANIFEST_VERSION,
            "processing_date": str(self.config.processing_date),
            "snapshot_id": self.config.snapshot_id,
            "config": self.config.as_manifest(),
            "schema_versions": {
                "normalized_gps": NORMALIZED_GPS_SCHEMA_VERSION,
                "schedule": SCHEDULE_SCHEMA_VERSION,
                "stop_semantics": SEMANTICS_SCHEMA_VERSION,
                "trip_universe": TRIP_UNIVERSE_SCHEMA_VERSION,
                "duty_execution": EXECUTION_SCHEMA_VERSION,
                "operational_stop_crossings": OPERATIONAL_CROSSING_SCHEMA_VERSION,
                "passenger_stop_arrivals": PASSENGER_ARRIVAL_SCHEMA_VERSION,
                "reconstruction_trip_facts": TRIP_FACT_SCHEMA_VERSION,
                "reconstruction_stop_arrivals": STOP_ARRIVAL_FACT_SCHEMA_VERSION,
                "reconstruction_expected_stop_events": EXPECTED_STOP_EVENT_SCHEMA_VERSION,
            },
            "inputs": {"gps": [_identity(file) for file in files], "gtfs_zip": _identity(self.config.gtfs_zip)},
            "outputs": {
                "normalized_gps": _identity(self.normalized_path, relative=True),
                "duty_schedule": _identity(work / "duty_schedule.parquet", relative=True),
                "stop_semantics": _identity(work / "stop_semantics.parquet", relative=True),
                "trip_universe": _identity(work / "trip_universe.parquet", relative=True),
                "duty_execution": _identity(work / "duty_execution.parquet", relative=True),
                "operational_stop_crossings": _identity(work / "operational_stop_crossings.parquet", relative=True),
                "passenger_stop_arrivals": _identity(work / "passenger_stop_arrivals.parquet", relative=True),
                "reconstruction_trip_facts": _identity(work / "reconstruction_trip_facts.parquet", relative=True),
                "reconstruction_stop_arrivals": _identity(work / "reconstruction_stop_arrivals.parquet", relative=True),
                "reconstruction_expected_stop_events": _identity(
                    work / "reconstruction_expected_stop_events.parquet", relative=True
                ),
            },
            "missing_hours": missing,
        }
        vehicle_stats = connection.execute(
            "select count(*), coalesce(max(group_rows), 0) from "
            "(select vehicle_number, count(*) group_rows from normalized_gps group by vehicle_number)"
        ).fetchone()
        artifact_bytes = sum(path.stat().st_size for path in work.iterdir() if path.is_file())
        process = _process_measurements()
        metrics = {
            "input_rows": input_rows,
            "normalized_rows": normalized_rows,
            "schedule_rows": schedule_rows,
            "hourly_rows": hourly_counts(connection),
            "wall_seconds": round(time.perf_counter() - started, 6),
            "cpu_seconds": round(time.process_time() - cpu_started, 6),
            "temp_disk_bytes": sum(path.stat().st_size for path in (work / "duckdb-temp").rglob("*") if path.is_file()),
            "artifact_disk_bytes": artifact_bytes,
            "vehicle_groups": int(vehicle_stats[0]) if vehicle_stats else 0,
            "max_vehicle_rows": int(vehicle_stats[1]) if vehicle_stats else 0,
            "duty_execution_rows": sum(execution_counts.values()),
            "duty_execution_status_counts": dict(sorted(execution_counts.items())),
            "operational_stop_crossings": crossing_counts["operational_stop_crossings"],
            "passenger_stop_arrivals": crossing_counts["passenger_stop_arrivals"],
            "stop_alignment_missing_stops": crossing_counts["missing_stops"],
            "stop_alignment_ambiguous_trips": crossing_counts["ambiguous_trips"],
            "stop_alignment_vehicle_groups": crossing_counts["vehicle_groups"],
            "stop_alignment_execution_trips": crossing_counts["execution_trips"],
            "stop_alignment_workers": self.config.alignment_workers,
            "stop_alignment_worker_chunks": crossing_counts["worker_chunks"],
            **fact_counts,
            "swapping_observed": None if process["current_swap_bytes"] is None else process["current_swap_bytes"] > 0,
            **process,
        }
        _write_json(work / "manifest.json", manifest)
        _write_json(work / "metrics.json", metrics)
        return {"manifest": manifest, "metrics": metrics}

    def build_facts(self) -> dict[str, int]:
        """Adapt accepted ownership and direct crossings into warehouse-shaped local facts."""
        work = self._work()
        try:
            counts = build_facts(
                self._connection(),
                executions=work / "duty_execution.parquet",
                semantics=work / "stop_semantics.parquet",
                arrivals=work / "passenger_stop_arrivals.parquet",
                normalized_gps=self.normalized_path or work / "normalized_gps.parquet",
                trip_universe=work / "trip_universe.parquet",
                output_dir=work,
            )
        except (duckdb.Error, ValueError) as exc:
            raise fail("invalid_output", f"fact construction failed: {exc}", 15) from exc
        expected_schemas = {
            "reconstruction_trip_facts.parquet": RECONSTRUCTION_TRIP_FACT_SCHEMA,
            "reconstruction_stop_arrivals.parquet": RECONSTRUCTION_STOP_ARRIVAL_SCHEMA,
            "reconstruction_expected_stop_events.parquet": RECONSTRUCTION_EXPECTED_STOP_EVENT_SCHEMA,
        }
        for name, schema in expected_schemas.items():
            if pq.read_schema(work / name) != schema:
                raise fail("invalid_output", f"fact artifact schema validation failed: {name}", 15)
        return counts

    def align_execution(self) -> dict[str, int]:
        """Persist per-vehicle evidence, then settle one bounded duty at a time."""
        work = self._work()
        evidence_path = work / ".traversal_evidence.parquet"
        evidence_writer = pq.ParquetWriter(evidence_path, TRAVERSAL_EVIDENCE_SCHEMA, compression="zstd")
        try:
            for stream in self.iter_vehicle_streams():
                pings = stream.pings.to_pylist()
                courses = self._schedule_rows_for_stream(stream)
                batch = []
                patterns: dict[tuple[object, ...], list[dict[str, Any]]] = {}
                for course in courses:
                    pattern = (
                        course["line"],
                        course["brigade"],
                        course["mode"],
                        course["origin_lat"],
                        course["origin_lon"],
                        course["destination_lat"],
                        course["destination_lon"],
                    )
                    patterns.setdefault(pattern, []).append(course)
                for pattern_courses in patterns.values():
                    pattern_evidence = extract_evidence(pattern_courses[0], pings)
                    for course in pattern_courses:
                        lineage = {
                            name: course[name]
                            for name in (
                                "service_date",
                                "processing_date",
                                "gtfs_snapshot_id",
                                "duty_chain_id",
                                "trip_id",
                            )
                        }
                        batch.extend({**item, **lineage} for item in pattern_evidence)
                if batch:
                    evidence_writer.write_table(pa.Table.from_pylist(batch, schema=TRAVERSAL_EVIDENCE_SCHEMA))
        finally:
            evidence_writer.close()

        output_path = work / "duty_execution.parquet"
        output_writer = pq.ParquetWriter(output_path, DUTY_EXECUTION_SCHEMA, compression="zstd")
        counts: dict[str, int] = {}
        all_outcomes: list[dict[str, Any]] = []
        try:
            for service_date, snapshot_id, duty_id in self._duty_keys():
                courses = self._schedule_rows_for_duty(service_date, snapshot_id, duty_id)
                evidence = self._evidence_for_duty(evidence_path, service_date, snapshot_id, duty_id)
                all_outcomes.extend(settle_duty(courses, evidence))
            all_outcomes = resolve_competing_ownership(all_outcomes)
            for start in range(0, len(all_outcomes), SEMANTICS_BATCH_ROWS):
                output_writer.write_table(
                    pa.Table.from_pylist(
                        all_outcomes[start : start + SEMANTICS_BATCH_ROWS], schema=DUTY_EXECUTION_SCHEMA
                    )
                )
            for outcome in all_outcomes:
                status = str(outcome["execution_status"])
                counts[status] = counts.get(status, 0) + 1
        finally:
            output_writer.close()
            evidence_path.unlink(missing_ok=True)
            (work / ".terminal_courses.parquet").unlink(missing_ok=True)
        return counts

    def align_stops(self) -> dict[str, int]:
        """Align active vehicles in deterministic chunks and merge private worker shards."""
        work = self._work()
        execution_path = str(work / "duty_execution.parquet").replace("'", "''")
        semantics_path = str(work / "stop_semantics.parquet").replace("'", "''")
        inputs_path = work / ".stop_alignment_inputs.parquet"
        inputs_sql = str(inputs_path).replace("'", "''")
        shard_dir = work / ".stop-alignment-shards"
        counts = _stop_alignment_counts()
        semantic_columns = ", ".join(
            f"semantics.{column} as semantic_{column}" for column in STOP_ALIGNMENT_SEMANTIC_COLUMNS
        )
        try:
            # Join the large semantics artifact once; ordered row groups let vehicle filters skip unrelated inputs.
            self._connection().execute(
                f"""
                copy (
                    select executions.*, {semantic_columns}
                    from read_parquet('{execution_path}') as executions
                    inner join read_parquet('{semantics_path}') as semantics
                        on executions.service_date = semantics.service_date
                        and executions.processing_date = semantics.processing_date
                        and executions.gtfs_snapshot_id = semantics.gtfs_snapshot_id
                        and executions.duty_chain_id = semantics.duty_chain_id
                        and executions.trip_id = semantics.trip_id
                    where executions.execution_status = 'executed'
                      and executions.confidence = 'high'
                      and executions.ownership_interval_start_time is not null
                      and executions.ownership_interval_end_time is not null
                    order by executions.vehicle_number, executions.ownership_interval_start_time,
                        executions.service_date, executions.duty_chain_id, executions.trip_order,
                        executions.trip_id, semantics.stop_sequence
                ) to '{inputs_sql}' (format parquet, compression zstd, row_group_size {STOP_ALIGNMENT_ROW_GROUP_ROWS})
                """
            )
            vehicle_numbers = [
                str(row[0])
                for row in self._connection()
                .execute(f"select distinct vehicle_number from read_parquet('{inputs_sql}') order by vehicle_number")
                .fetchall()
            ]
            chunks = _balanced_vehicle_chunks(vehicle_numbers, self.config.alignment_workers)
            workers = [
                StopAlignmentWorker(
                    index,
                    chunk,
                    self.normalized_path or work / "normalized_gps.parquet",
                    inputs_path,
                    shard_dir,
                    self.config.memory_limit,
                    self.config.temp_limit,
                    self.config.max_vehicle_rows,
                )
                for index, chunk in enumerate(chunks)
            ]
            if len(workers) == 1:
                worker_counts = [_run_stop_alignment_worker(workers[0])]
            elif workers:
                with ProcessPoolExecutor(max_workers=len(workers)) as executor:
                    worker_counts = [
                        future.result()
                        for future in (executor.submit(_run_stop_alignment_worker, worker) for worker in workers)
                    ]
            else:
                worker_counts = []
            for worker_count in worker_counts:
                for key in counts:
                    counts[key] += worker_count[key]
            counts["worker_chunks"] = len(workers)
            _merge_stop_alignment_shards(
                shard_dir,
                len(workers),
                work / "operational_stop_crossings.parquet",
                STOP_CROSSING_SCHEMA,
                "operational",
            )
            _merge_stop_alignment_shards(
                shard_dir,
                len(workers),
                work / "passenger_stop_arrivals.parquet",
                PASSENGER_STOP_ARRIVAL_SCHEMA,
                "passenger",
            )
        finally:
            inputs_path.unlink(missing_ok=True)
            shutil.rmtree(shard_dir, ignore_errors=True)
        return counts

    def _schedule_rows_for_stream(self, stream: VehicleStream) -> list[dict[str, Any]]:
        """Read only courses whose line/brigade/mode occurs in this vehicle stream."""
        connection = self._connection()
        courses = self._work() / ".terminal_courses.parquet"
        connection.register("alignment_stream", stream.pings)
        try:
            return (
                connection.execute(
                    f"""
                with stream_lines as (
                    select distinct line, brigade, vehicle_type from alignment_stream
                )
                select courses.*
                from read_parquet('{str(courses).replace("'", "''")}') as courses
                inner join stream_lines on courses.line = stream_lines.line
                    and courses.brigade = stream_lines.brigade
                    and ((courses.mode = 'bus' and stream_lines.vehicle_type = 1)
                        or (courses.mode = 'tram' and stream_lines.vehicle_type = 2))
                order by courses.service_date, courses.duty_chain_id, courses.trip_order, courses.trip_id
                """
                )
                .to_arrow_table()
                .to_pylist()
            )
        finally:
            connection.unregister("alignment_stream")

    def _duty_keys(self) -> list[tuple[object, str, str]]:
        schedule = str(self._work() / "duty_schedule.parquet").replace("'", "''")
        return [
            (row[0], str(row[1]), str(row[2]))
            for row in self._connection()
            .execute(
                f"select distinct service_date, gtfs_snapshot_id, duty_chain_id from read_parquet('{schedule}') "
                "order by service_date, gtfs_snapshot_id, duty_chain_id"
            )
            .fetchall()
        ]

    def _schedule_rows_for_duty(self, service_date: object, snapshot_id: str, duty_id: str) -> list[dict[str, Any]]:
        courses = self._work() / ".terminal_courses.parquet"
        return (
            self._connection()
            .execute(
                f"""
            select * from read_parquet('{str(courses).replace("'", "''")}')
            where service_date = ? and gtfs_snapshot_id = ? and duty_chain_id = ?
            order by trip_order, trip_id
            """,
                [service_date, snapshot_id, duty_id],
            )
            .to_arrow_table()
            .to_pylist()
        )

    def _evidence_for_duty(
        self, path: Path, service_date: object, snapshot_id: str, duty_id: str
    ) -> list[dict[str, Any]]:
        return (
            self._connection()
            .execute(
                f"""
            select * from read_parquet('{str(path).replace("'", "''")}')
            where service_date = ? and gtfs_snapshot_id = ? and duty_chain_id = ?
            order by trip_id, vehicle_number, candidate_kind, origin_event_time, traversal_id
            """,
                [service_date, snapshot_id, duty_id],
            )
            .to_arrow_table()
            .to_pylist()
        )

    def iter_vehicle_streams(self) -> Iterator[VehicleStream]:
        """Fetch exactly one vehicle group per query, in deterministic order."""
        path = self.normalized_path or self._work() / "normalized_gps.parquet"
        if not path.is_file():
            raise fail("invalid_output", "normalized GPS must be prepared before iteration", 15)
        quoted = str(path).replace("'", "''")
        connection = self._connection()
        for (vehicle,) in connection.execute(
            f"select distinct vehicle_number from read_parquet('{quoted}') order by vehicle_number"
        ).fetchall():
            count = connection.execute(
                f"select count(*) from read_parquet('{quoted}') where vehicle_number = ?", [vehicle]
            ).fetchone()
            if count and int(count[0]) > self.config.max_vehicle_rows:
                raise fail("resource_limit", f"vehicle {vehicle} exceeds max_vehicle_rows", 14)
            table = connection.execute(
                f"select * from read_parquet('{quoted}') where vehicle_number = ? "
                "order by gps_time, ingested_at, line, brigade",
                [vehicle],
            ).to_arrow_table()
            yield VehicleStream(str(vehicle), table)

    def _publish(self) -> None:
        work = self._work()
        if self.config.output_dir.exists():
            raise fail("invalid_output", f"output directory already exists: {self.config.output_dir}", 15)
        shutil.move(str(work), str(self.config.output_dir))
        if self.config.metrics_json != self.config.output_dir / "metrics.json":
            self.config.metrics_json.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(self.config.output_dir / "metrics.json", self.config.metrics_json)

    def _work(self) -> Path:
        if self.work_dir is None:
            raise RuntimeError("ReconstructionRun must be used as a context manager")
        return self.work_dir

    def _connection(self) -> duckdb.DuckDBPyConnection:
        if self.connection is None:
            raise RuntimeError("ReconstructionRun must be used as a context manager")
        return self.connection
