"""Bounded DuckDB runtime and grouped vehicle seam."""

import hashlib
import json
import os
import shutil
import time
import uuid
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from ztm_matcher.alignment import extract_evidence, settle_duty, terminal_courses
from ztm_matcher.config import RunConfig
from ztm_matcher.errors import fail
from ztm_matcher.gps import discover, hourly_counts, normalize
from ztm_matcher.gtfs import load, select
from ztm_matcher.schemas import (
    DUTY_EXECUTION_SCHEMA,
    EXECUTION_SCHEMA_VERSION,
    MANIFEST_VERSION,
    NORMALIZED_GPS_SCHEMA,
    NORMALIZED_GPS_SCHEMA_VERSION,
    SCHEDULE_SCHEMA_VERSION,
    SEMANTICS_SCHEMA_VERSION,
    TRAVERSAL_EVIDENCE_SCHEMA,
)
from ztm_matcher.semantics import duties, iter_stop_semantics

SEMANTICS_BATCH_ROWS = 10_000


@dataclass(frozen=True)
class VehicleStream:
    """One bounded, deterministically ordered vehicle GPS group."""

    vehicle_number: str
    pings: pa.Table


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
        for semantic in iter_stop_semantics(rows, snapshot):
            semantics_batch.append(semantic)
            if len(semantics_batch) < SEMANTICS_BATCH_ROWS:
                continue
            table = pa.Table.from_pylist(semantics_batch)
            semantics_columns = set(table.column_names)
            semantics_writer = semantics_writer or pq.ParquetWriter(semantics_path, table.schema, compression="zstd")
            semantics_writer.write_table(table)
            semantics_count += table.num_rows
            semantics_batch.clear()
        if semantics_batch:
            table = pa.Table.from_pylist(semantics_batch)
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
        return len(rows)

    def prepare(self) -> dict[str, Any]:
        """Produce validated artifacts plus deterministic lineage and resource metrics."""
        started = time.perf_counter()
        cpu_started = time.process_time()
        work, connection = self._work(), self._connection()
        files, missing = discover(self.config.gps_root, self.config.processing_date)
        missing_modes = {mode: hours for mode, hours in missing.items() if hours}
        if missing_modes and not self.config.allow_missing_hours:
            detail = "; ".join(
                f"{mode}: {','.join(f'{hour:02d}' for hour in hours)}" for mode, hours in missing_modes.items()
            )
            raise fail("missing_input", f"GPS input has missing hourly partitions ({detail})", 10)
        schedule_rows = self.prepare_schedule()
        self.normalized_path = work / "normalized_gps.parquet"
        normalized_rows = normalize(connection, files, self.config.processing_date, self.normalized_path)
        execution_counts = self.align_execution()
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
                "duty_execution": EXECUTION_SCHEMA_VERSION,
            },
            "inputs": {"gps": [_identity(file) for file in files], "gtfs_zip": _identity(self.config.gtfs_zip)},
            "outputs": {
                "normalized_gps": _identity(self.normalized_path, relative=True),
                "duty_schedule": _identity(work / "duty_schedule.parquet", relative=True),
                "stop_semantics": _identity(work / "stop_semantics.parquet", relative=True),
                "duty_execution": _identity(work / "duty_execution.parquet", relative=True),
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
            "swapping_observed": None if process["current_swap_bytes"] is None else process["current_swap_bytes"] > 0,
            **process,
        }
        _write_json(work / "manifest.json", manifest)
        _write_json(work / "metrics.json", metrics)
        return {"manifest": manifest, "metrics": metrics}

    def align_execution(self) -> dict[str, int]:
        """Persist per-vehicle evidence, then settle one bounded duty at a time."""
        work = self._work()
        evidence_path = work / ".traversal_evidence.parquet"
        evidence_writer = pq.ParquetWriter(evidence_path, TRAVERSAL_EVIDENCE_SCHEMA, compression="zstd")
        try:
            for stream in self.iter_vehicle_streams():
                pings = stream.pings.to_pylist()
                rows = self._schedule_rows_for_stream(stream)
                batch = []
                courses = terminal_courses(rows)
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
        try:
            for service_date, snapshot_id, duty_id in self._duty_keys():
                courses = terminal_courses(self._schedule_rows_for_duty(service_date, snapshot_id, duty_id))
                evidence = self._evidence_for_duty(evidence_path, service_date, snapshot_id, duty_id)
                outcomes = settle_duty(courses, evidence)
                output_writer.write_table(pa.Table.from_pylist(outcomes, schema=DUTY_EXECUTION_SCHEMA))
                for outcome in outcomes:
                    status = str(outcome["execution_status"])
                    counts[status] = counts.get(status, 0) + 1
        finally:
            output_writer.close()
            evidence_path.unlink(missing_ok=True)
        return counts

    def _schedule_rows_for_stream(self, stream: VehicleStream) -> list[dict[str, Any]]:
        """Read only courses whose line/brigade/mode occurs in this vehicle stream."""
        connection = self._connection()
        schedule, semantics = self._work() / "duty_schedule.parquet", self._work() / "stop_semantics.parquet"
        connection.register("alignment_stream", stream.pings)
        try:
            return (
                connection.execute(
                    f"""
                with stream_lines as (
                    select distinct line, brigade, vehicle_type from alignment_stream
                )
                select schedule.*, semantics.stop_id, semantics.stop_sequence, semantics.stop_lat, semantics.stop_lon,
                    semantics.is_passenger_stop, semantics.are_passenger_boundaries_settled
                from read_parquet('{str(schedule).replace("'", "''")}') as schedule
                inner join stream_lines on schedule.line = stream_lines.line
                    and schedule.brigade = stream_lines.brigade
                    and ((schedule.mode = 'bus' and stream_lines.vehicle_type = 1)
                        or (schedule.mode = 'tram' and stream_lines.vehicle_type = 2))
                inner join read_parquet('{str(semantics).replace("'", "''")}') as semantics
                    on schedule.service_date = semantics.service_date
                    and schedule.processing_date = semantics.processing_date
                    and schedule.gtfs_snapshot_id = semantics.gtfs_snapshot_id
                    and schedule.duty_chain_id = semantics.duty_chain_id
                    and schedule.trip_id = semantics.trip_id
                order by schedule.service_date, schedule.duty_chain_id, schedule.trip_order, schedule.trip_id,
                    semantics.stop_sequence, semantics.stop_id
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
        schedule, semantics = self._work() / "duty_schedule.parquet", self._work() / "stop_semantics.parquet"
        return (
            self._connection()
            .execute(
                f"""
            select schedule.*, semantics.stop_id, semantics.stop_sequence, semantics.stop_lat, semantics.stop_lon,
                semantics.is_passenger_stop, semantics.are_passenger_boundaries_settled
            from read_parquet('{str(schedule).replace("'", "''")}') as schedule
            inner join read_parquet('{str(semantics).replace("'", "''")}') as semantics
                on schedule.service_date = semantics.service_date
                and schedule.processing_date = semantics.processing_date
                and schedule.gtfs_snapshot_id = semantics.gtfs_snapshot_id
                and schedule.duty_chain_id = semantics.duty_chain_id
                and schedule.trip_id = semantics.trip_id
            where schedule.service_date = ? and schedule.gtfs_snapshot_id = ? and schedule.duty_chain_id = ?
            order by schedule.trip_order, schedule.trip_id, semantics.stop_sequence, semantics.stop_id
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
