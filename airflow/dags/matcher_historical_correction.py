"""Read-only, manually invoked planner for bounded matcher historical corrections."""

from __future__ import annotations

import argparse
import json
import os
import re
import shlex
import subprocess
from datetime import date, timedelta
from pathlib import Path
from time import monotonic, sleep
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlparse

from google.api_core.exceptions import PreconditionFailed
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_INT_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    HISTORICAL_DAILY_ELIGIBLE_START_DATE,
    HISTORICAL_DAILY_EXCLUSION_REASONS,
    RAW_GPS_PREFIX,
    historical_daily_exclusion_reason,
    matcher_input_inventory_digest,
)

if TYPE_CHECKING:
    from collections.abc import Callable

DEFAULT_MAX_DAYS = 31
MAX_SERVING_REFRESH_DAYS = 32
DEFAULT_MAX_QUERY_BYTES = 5 * 1024**3
DEFAULT_RUN_TIMEOUT_SECONDS = 2 * 60 * 60
DEFAULT_RUN_POLL_INTERVAL_SECONDS = 15
PLAN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
PROCESSING_SNAPSHOT_TABLE = f"{GCP_PROJECT}.{BIGQUERY_INT_DATASET}.int_gtfs_processing_snapshot"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"


def _positive_env(name: str, default: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if value < 1:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _row_value(row: object, name: str) -> object:
    if isinstance(row, dict):
        return row.get(name)
    return getattr(row, name, None)


def _gcs_identity(blob: object) -> dict[str, object]:
    name, generation, size = (
        str(getattr(blob, "name", "")),
        getattr(blob, "generation", None),
        getattr(blob, "size", None),
    )
    md5_hash, crc32c = getattr(blob, "md5_hash", None), getattr(blob, "crc32c", None)
    if not name or generation is None or size is None or not (md5_hash or crc32c):
        raise RuntimeError("Historical correction inventory requires GCS generation, byte size, and hash")
    return {
        "name": name,
        "generation": str(generation),
        "size": int(size),
        "md5_hash": str(md5_hash) if md5_hash else None,
        "crc32c": str(crc32c) if crc32c else None,
        "bytes": int(size),
    }


def _snapshot_rows(client: Any, start: date, end: date, maximum_bytes_billed: int) -> dict[str, dict[str, str]]:
    query = f"""
        select mapping.processing_date, mapping.gtfs_snapshot_id, snapshots.gcs_path
        from `{PROCESSING_SNAPSHOT_TABLE}` as mapping
        inner join `{RAW_GTFS_SNAPSHOTS_TABLE}` as snapshots
            on mapping.gtfs_snapshot_id = snapshots.snapshot_id
        where mapping.processing_date between @start_date and @end_date
        order by mapping.processing_date
    """
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start),
            bigquery.ScalarQueryParameter("end_date", "DATE", end),
        ],
        maximum_bytes_billed=maximum_bytes_billed,
    )
    mappings: dict[str, dict[str, str]] = {}
    for row in client.query(query, job_config=config).result():
        processing_date = str(_row_value(row, "processing_date"))
        snapshot_id, gcs_path = _row_value(row, "gtfs_snapshot_id"), _row_value(row, "gcs_path")
        if processing_date in mappings or not snapshot_id or not gcs_path:
            raise RuntimeError(f"No exact usable GTFS snapshot mapping for {processing_date}")
        mappings[processing_date] = {"gtfs_snapshot_id": str(snapshot_id), "gcs_path": str(gcs_path)}
    return mappings


def _snapshot_inventory(client: Any, uri: str) -> dict[str, object]:
    parsed = urlparse(uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise RuntimeError(f"Mapped GTFS snapshot is not a GCS URI: {uri}")
    blob = client.bucket(parsed.netloc).get_blob(parsed.path.removeprefix("/"))
    if blob is None:
        raise RuntimeError(f"Mapped GTFS snapshot is missing: {uri}")
    return _gcs_identity(blob) | {"gcs_path": uri}


def _gps_inventory(
    client: Any, input_dates: tuple[str, ...]
) -> tuple[list[dict[str, Any]], dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
    """Inventory every actual GPS input date, preserving date-level lineage."""
    bucket = client.bucket(GCS_BUCKET)
    objects: list[dict[str, Any]] = []
    by_mode: dict[str, dict[str, Any]] = {}
    by_input_date: dict[str, dict[str, Any]] = {}
    for input_date in input_dates:
        by_input_date[input_date] = {}
        for mode in ("bus", "tram"):
            prefix = f"{RAW_GPS_PREFIX}/vehicle_type={mode}/date={input_date}/"
            mode_objects = [
                cast("dict[str, Any]", _gcs_identity(blob) | {"mode": mode, "gps_date": input_date})
                for blob in bucket.list_blobs(prefix=prefix)
                if str(getattr(blob, "name", "")).endswith(".parquet") and "/part-" in str(getattr(blob, "name", ""))
            ]
            if not mode_objects:
                raise RuntimeError(f"GPS input inventory is missing {mode} objects for {input_date}")
            mode_objects.sort(key=lambda item: str(item["name"]))
            objects.extend(mode_objects)
            by_input_date[input_date][mode] = {
                "objects": mode_objects,
                "count": len(mode_objects),
                "bytes": sum(int(item["bytes"]) for item in mode_objects),
            }
    for mode in ("bus", "tram"):
        mode_objects = [item for item in objects if item["mode"] == mode]
        by_mode[mode] = {
            "objects": mode_objects,
            "count": len(mode_objects),
            "bytes": sum(int(item["bytes"]) for item in mode_objects),
        }
    names = [str(item["name"]) for item in objects]
    if len(names) != len(set(names)):
        raise RuntimeError("GPS input inventory contains duplicate object paths")
    return sorted(objects, key=lambda item: str(item["name"])), by_mode, by_input_date


def _preflight_command(processing_date: str, gtfs_snapshot_id: str, maximum_bytes_billed: int) -> str:
    query = f"""
        select mapping.processing_date, mapping.gtfs_snapshot_id, snapshots.gcs_path
        from `{PROCESSING_SNAPSHOT_TABLE}` as mapping
        inner join `{RAW_GTFS_SNAPSHOTS_TABLE}` as snapshots
            on mapping.gtfs_snapshot_id = snapshots.snapshot_id
        where mapping.processing_date = @processing_date
          and mapping.gtfs_snapshot_id = @gtfs_snapshot_id
    """
    return " ".join(
        [
            "bq query --use_legacy_sql=false --dry_run",
            f"--maximum_bytes_billed={maximum_bytes_billed}",
            f"--parameter=processing_date:DATE:{processing_date}",
            f"--parameter=gtfs_snapshot_id:STRING:{shlex.quote(gtfs_snapshot_id)}",
            shlex.quote(query),
        ]
    )


def _inventory_command(day: dict[str, Any]) -> str:
    uris = [str(day["gtfs_snapshot"]["gcs_path"])]
    uris.extend(f"gs://{GCS_BUCKET}/{item['name']}" for item in day["gps_inventory"])
    return "gcloud storage ls " + " ".join(shlex.quote(uri) for uri in uris)


def _validated_plan_id(plan_id: str) -> str:
    if not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise ValueError("plan_id must be 1-64 characters of letters, digits, underscores, or hyphens")
    return plan_id


def _historical_run_id(plan_id: str, processing_date: str) -> str:
    return f"matcher-historical-correction__{plan_id}__{processing_date}"


def _execution_command(  # noqa: PLR0913
    plan_id: str,
    processing_date: str,
    gtfs_snapshot_id: str,
    expected_input_inventory_digest: str,
    maximum_bytes_billed: int,
    *,
    skip_prior_publication: bool,
) -> str:
    conf: dict[str, object] = {
        "processing_date": processing_date,
        "expected_gtfs_snapshot_id": gtfs_snapshot_id,
        "expected_input_inventory_digest": expected_input_inventory_digest,
        "historical_correction": True,
        "historical_plan_id": plan_id,
        "maximum_bytes_billed": maximum_bytes_billed,
    }
    if skip_prior_publication:
        conf["skip_prior_publication"] = True
    return " ".join(
        [
            "airflow dags trigger dag_daily_gps",
            f"--run-id {shlex.quote(_historical_run_id(plan_id, processing_date))}",
            f"--conf {shlex.quote(json.dumps(conf, sort_keys=True))}",
        ]
    )


def _serving_refresh_run_id(plan_id: str) -> str:
    return f"matcher-historical-serving-refresh__{plan_id}"


def _serving_refresh_command(plan_id: str, days: list[dict[str, str]], maximum_bytes_billed: int) -> str:
    conf = {
        "days": days,
        "restore_day": days[-1],
        "maximum_bytes_billed": maximum_bytes_billed,
        "historical_plan_id": plan_id,
    }
    return " ".join(
        [
            "airflow dags trigger dag_historical_serving_refresh",
            f"--run-id {shlex.quote(_serving_refresh_run_id(plan_id))}",
            f"--conf {shlex.quote(json.dumps(conf, sort_keys=True))}",
        ]
    )


def _wait_command(plan_id: str, processing_date: str, timeout_seconds: int, poll_interval_seconds: int) -> str:
    return " ".join(
        [
            f"python {shlex.quote(str(Path(__file__).resolve()))} wait-for-dag-run",
            "--dag-id dag_daily_gps",
            f"--run-id {shlex.quote(_historical_run_id(plan_id, processing_date))}",
            f"--timeout-seconds {timeout_seconds}",
            f"--poll-interval-seconds {poll_interval_seconds}",
        ]
    )


def _raw_processing_date_exclusion(processing_date: date) -> dict[str, str] | None:
    raw_reason = historical_daily_exclusion_reason(processing_date)
    if raw_reason:
        return {
            "processing_date": processing_date.isoformat(),
            "reason": "raw_processing_date_excluded",
            "raw_exclusion_reason": raw_reason,
        }

    return None


def _prior_publication_boundary(processing_date: date) -> dict[str, str] | None:
    prior_service_date = processing_date - timedelta(days=1)
    prior_reason = historical_daily_exclusion_reason(prior_service_date)
    if prior_reason:
        return {
            "processing_date": processing_date.isoformat(),
            "reason": "prior_service_date_excluded",
            "prior_service_date": prior_service_date.isoformat(),
            "prior_raw_exclusion_reason": prior_reason,
        }
    return None


def _eligible_processing_dates(start: date, end: date) -> tuple[list[date], list[dict[str, str]]]:
    eligible: list[date] = []
    excluded: list[dict[str, str]] = []
    for index in range((end - start).days + 1):
        processing_date = start + timedelta(days=index)
        exclusion = _raw_processing_date_exclusion(processing_date)
        if exclusion:
            excluded.append(exclusion)
        else:
            eligible.append(processing_date)
    return eligible, excluded


def _processing_date_segments(processing_dates: list[date]) -> list[dict[str, object]]:
    if not processing_dates:
        return []
    segments: list[dict[str, object]] = []
    start = end = processing_dates[0]
    for processing_date in processing_dates[1:]:
        if processing_date == end + timedelta(days=1):
            end = processing_date
            continue
        segments.append({"start_date": start.isoformat(), "end_date": end.isoformat(), "days": (end - start).days + 1})
        start = end = processing_date
    segments.append({"start_date": start.isoformat(), "end_date": end.isoformat(), "days": (end - start).days + 1})
    return segments


def _dag_run_state(dag_id: str, run_id: str) -> str | None:
    """Read a single Airflow DAG run state without modifying metadata."""
    from airflow.models import DagRun  # noqa: PLC0415
    from airflow.utils.session import create_session  # noqa: PLC0415
    from sqlalchemy import select  # noqa: PLC0415

    with create_session() as session:
        state = session.scalar(select(DagRun.state).where(DagRun.dag_id == dag_id, DagRun.run_id == run_id))
    return str(getattr(state, "value", state)) if state is not None else None


def wait_for_dag_run(  # noqa: PLR0913
    dag_id: str,
    run_id: str,
    *,
    timeout_seconds: int,
    poll_interval_seconds: int,
    get_state: Callable[[str, str], str | None] = _dag_run_state,
    clock: Callable[[], float] = monotonic,
    sleep_for: Callable[[float], None] = sleep,
) -> None:
    """Wait for one DAG run, raising on failure or a bounded timeout."""
    if timeout_seconds < 1 or poll_interval_seconds < 1:
        raise ValueError("DAG run wait timeout and poll interval must be positive")
    deadline = clock() + timeout_seconds
    while True:
        state = get_state(dag_id, run_id)
        if state == "success":
            return
        if state == "failed":
            raise RuntimeError(f"DAG run failed: dag_id={dag_id} run_id={run_id}")
        remaining = deadline - clock()
        if remaining <= 0:
            raise TimeoutError(f"Timed out waiting for DAG run: dag_id={dag_id} run_id={run_id}")
        sleep_for(min(poll_interval_seconds, remaining))


def _run_command(command: str) -> None:
    subprocess.run(shlex.split(command), check=True)  # noqa: S603


def execute_historical_correction_plan(  # noqa: C901
    plan: dict[str, object],
    *,
    get_state: Callable[[str, str], str | None] = _dag_run_state,
    run_command: Callable[[str], None] = _run_command,
    wait_for_run: Callable[[str, str], None] | None = None,
) -> None:
    """Run an approved plan sequentially, skipping already successful deterministic runs."""
    if plan.get("plan_version") != "matcher-historical-correction-v6" or plan.get("read_only") is not True:
        raise ValueError("Historical correction execution requires an approved v6 plan")
    bounds = cast("dict[str, int]", plan["bounds"])
    timeout_seconds = int(bounds["run_timeout_seconds"])
    poll_interval_seconds = int(bounds["run_poll_interval_seconds"])

    def wait(dag_id: str, run_id: str) -> None:
        if wait_for_run is not None:
            wait_for_run(dag_id, run_id)
            return
        wait_for_dag_run(
            dag_id,
            run_id,
            timeout_seconds=timeout_seconds,
            poll_interval_seconds=poll_interval_seconds,
        )

    commands = cast("list[str]", plan["sequential_commands"])
    days = cast("list[dict[str, object]]", plan["days"])
    for index, day in enumerate(days):
        run_id = _historical_run_id(str(plan["plan_id"]), str(day["processing_date"]))
        state = get_state("dag_daily_gps", run_id)
        if state == "success":
            continue
        if state in {"queued", "running"}:
            wait("dag_daily_gps", run_id)
            continue
        if state is not None:
            raise RuntimeError(
                f"Historical correction run must be cleared before resume: run_id={run_id} state={state}"
            )
        run_command(commands[index * 4 + 2])
        wait("dag_daily_gps", run_id)

    refresh = cast("dict[str, object]", plan["serving_refresh"])
    refresh_run_id = str(refresh["run_id"])
    refresh_state = get_state("dag_historical_serving_refresh", refresh_run_id)
    if refresh_state == "success":
        return
    if refresh_state in {"queued", "running"}:
        wait("dag_historical_serving_refresh", refresh_run_id)
        return
    if refresh_state is not None:
        raise RuntimeError(
            f"Historical serving refresh must be cleared before resume: run_id={refresh_run_id} state={refresh_state}"
        )
    run_command(commands[-2])
    wait("dag_historical_serving_refresh", refresh_run_id)


def build_historical_correction_plan(  # noqa: C901, PLR0913
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    plan_id: str,
    refresh_through_date: str | None = None,
    bq_client: Any | None = None,
    storage_client: Any | None = None,
) -> dict[str, object]:
    """Build a deterministic plan without downloading, loading, publishing, or writing cloud data."""
    if start_date is None or end_date is None:
        raise ValueError("Historical correction planning requires explicit start_date and end_date")
    plan_id = _validated_plan_id(plan_id)
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    if refresh_through_date is None:
        raise ValueError("Historical correction planning requires explicit refresh_through_date")
    refresh_end = date.fromisoformat(refresh_through_date)
    max_days = _positive_env("MATCHER_HISTORICAL_MAX_DAYS", DEFAULT_MAX_DAYS)
    maximum_bytes_billed = _positive_env("MATCHER_HISTORICAL_MAX_QUERY_BYTES", DEFAULT_MAX_QUERY_BYTES)
    run_timeout_seconds = _positive_env("MATCHER_HISTORICAL_RUN_TIMEOUT_SECONDS", DEFAULT_RUN_TIMEOUT_SECONDS)
    run_poll_interval_seconds = _positive_env(
        "MATCHER_HISTORICAL_RUN_POLL_INTERVAL_SECONDS", DEFAULT_RUN_POLL_INTERVAL_SECONDS
    )
    if (
        start > end
        or refresh_end < end
        or start < HISTORICAL_DAILY_ELIGIBLE_START_DATE
        or (end - start).days + 1 > max_days
    ):
        raise ValueError("Historical correction date range is outside its allowed bounded window")
    planned_dates, skipped_processing_dates = _eligible_processing_dates(start, end)
    if not planned_dates:
        raise ValueError("Historical correction date range has no eligible processing dates")
    bq_client = bq_client or bigquery.Client(project=GCP_PROJECT)
    storage_client = storage_client or storage.Client(project=GCP_PROJECT)
    mappings = _snapshot_rows(bq_client, start - timedelta(days=1), refresh_end, maximum_bytes_billed)
    missing = [item.isoformat() for item in planned_dates if item.isoformat() not in mappings]
    if missing:
        raise RuntimeError(f"No exact GTFS snapshot mapping for: {', '.join(missing)}")
    days: list[dict[str, Any]] = []
    for processing_day in planned_dates:
        processing_date = processing_day.isoformat()
        mapping = mappings[processing_date]
        boundary = _prior_publication_boundary(processing_day)
        include_prior_gps = boundary is None
        input_dates = tuple(
            item.isoformat()
            for item in (
                (processing_day - timedelta(days=1), processing_day) if include_prior_gps else (processing_day,)
            )
        )
        gps_objects, gps_inventory_by_mode, gps_inventory_by_input_date = _gps_inventory(storage_client, input_dates)
        snapshot = _snapshot_inventory(storage_client, mapping["gcs_path"])
        inventory_digest = matcher_input_inventory_digest(
            processing_date=processing_date,
            snapshot_id=mapping["gtfs_snapshot_id"],
            snapshot_gcs_path=mapping["gcs_path"],
            include_prior_gps=include_prior_gps,
            input_dates=input_dates,
            gtfs_object=snapshot,
            gps_objects=gps_objects,
        )
        affected_partitions = {"current_service_date": processing_date}
        if include_prior_gps:
            affected_partitions["prior_service_date"] = (processing_day - timedelta(days=1)).isoformat()
        days.append(
            {
                "processing_date": processing_date,
                "gtfs_snapshot_id": mapping["gtfs_snapshot_id"],
                "gtfs_snapshot": snapshot,
                "gps_inventory": gps_objects,
                "gps_inventory_by_mode": gps_inventory_by_mode,
                "gps_inventory_by_input_date": gps_inventory_by_input_date,
                "include_prior_gps": include_prior_gps,
                "input_dates": list(input_dates),
                "expected_input_inventory_digest": inventory_digest,
                "publication_mode": "current_only" if not include_prior_gps else "current_and_prior",
                "prior_publication_boundary": boundary,
                "affected_partitions": affected_partitions,
            }
        )
    gps_bytes = sum(int(gps_object["bytes"]) for planned_day in days for gps_object in planned_day["gps_inventory"])
    gtfs_bytes = sum(int(planned_day["gtfs_snapshot"]["bytes"]) for planned_day in days)
    first_affected_date = min(
        date.fromisoformat(str(service_date))
        for planned_day in days
        for service_date in cast("dict[str, str]", planned_day["affected_partitions"]).values()
    )
    affected_service_dates = [
        item.isoformat()
        for index in range((refresh_end - first_affected_date).days + 1)
        if not historical_daily_exclusion_reason(item := first_affected_date + timedelta(days=index))
    ]
    if len(affected_service_dates) > MAX_SERVING_REFRESH_DAYS:
        raise ValueError(f"Historical serving refresh exceeds its bounded {MAX_SERVING_REFRESH_DAYS}-day window")
    missing_refresh_mappings = [service_date for service_date in affected_service_dates if service_date not in mappings]
    if missing_refresh_mappings:
        raise RuntimeError(f"No exact GTFS snapshot mapping for serving refresh: {', '.join(missing_refresh_mappings)}")
    serving_refresh_days = [
        {"processing_date": service_date, "gtfs_snapshot_id": mappings[service_date]["gtfs_snapshot_id"]}
        for service_date in affected_service_dates
    ]
    sequential_commands: list[str] = []
    for planned_day in days:
        sequential_commands.extend(
            [
                _preflight_command(
                    str(planned_day["processing_date"]),
                    str(planned_day["gtfs_snapshot_id"]),
                    maximum_bytes_billed,
                ),
                _inventory_command(planned_day),
                _execution_command(
                    plan_id,
                    str(planned_day["processing_date"]),
                    str(planned_day["gtfs_snapshot_id"]),
                    str(planned_day["expected_input_inventory_digest"]),
                    maximum_bytes_billed,
                    skip_prior_publication=planned_day["prior_publication_boundary"] is not None,
                ),
                _wait_command(
                    plan_id,
                    str(planned_day["processing_date"]),
                    run_timeout_seconds,
                    run_poll_interval_seconds,
                ),
            ]
        )
    sequential_commands.extend(
        [
            _serving_refresh_command(plan_id, serving_refresh_days, maximum_bytes_billed),
            _wait_command(
                plan_id,
                affected_service_dates[-1],
                run_timeout_seconds,
                run_poll_interval_seconds,
            )
            .replace("dag_daily_gps", "dag_historical_serving_refresh")
            .replace(_historical_run_id(plan_id, affected_service_dates[-1]), _serving_refresh_run_id(plan_id)),
        ]
    )
    return {
        "plan_version": "matcher-historical-correction-v6",
        "plan_id": plan_id,
        "read_only": True,
        "date_range": {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "requested_days": (end - start).days + 1,
            "days": len(days),
        },
        "bounds": {
            "eligible_start_date": HISTORICAL_DAILY_ELIGIBLE_START_DATE.isoformat(),
            "excluded_dates": {item.isoformat(): reason for item, reason in HISTORICAL_DAILY_EXCLUSION_REASONS.items()},
            "max_days": max_days,
            "maximum_bytes_billed": maximum_bytes_billed,
            "run_timeout_seconds": run_timeout_seconds,
            "run_poll_interval_seconds": run_poll_interval_seconds,
        },
        "estimated_input_totals": {
            "gps_objects": sum(len(item["gps_inventory"]) for item in days),
            "gps_bytes": gps_bytes,
            "gtfs_bytes": gtfs_bytes,
            "total_bytes": gps_bytes + gtfs_bytes,
        },
        "days": days,
        "serving_refresh": {
            "days": serving_refresh_days,
            "affected_service_dates": affected_service_dates,
            "run_id": _serving_refresh_run_id(plan_id),
            "runs_after_all_corrections": True,
        },
        "eligible_processing_segments": _processing_date_segments(planned_dates),
        "skipped_processing_dates": skipped_processing_dates,
        "sequential_commands": sequential_commands,
        "rollback_boundary": "No canonical data is changed by planning. Any future correction must stop before its first approved partition replacement; restoring prior canonical partitions is outside this tool.",
    }


def write_plan_report(plan: dict[str, object], path: Path) -> None:
    """Write a requested local report; planning itself never writes one."""
    path.write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def upload_plan_report(plan: dict[str, object], client: Any, gcs_uri: str) -> None:
    """Create a requested report once; this is the planner's sole optional cloud mutation."""
    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError("GCS report URI must be gs://bucket/object")
    try:
        client.bucket(parsed.netloc).blob(parsed.path.removeprefix("/")).upload_from_string(
            json.dumps(plan, sort_keys=True, indent=2) + "\n",
            content_type="application/json",
            if_generation_match=0,
        )
    except PreconditionFailed as exc:
        raise RuntimeError("Historical correction plan report already exists; choose a new GCS URI") from exc


def main(argv: list[str] | None = None) -> int:
    """Run the read-only planner or wait for one manually triggered DAG run."""
    parser = argparse.ArgumentParser(prog="matcher-historical-correction")
    subparsers = parser.add_subparsers(dest="mode", required=True)
    plan_parser = subparsers.add_parser("plan")
    plan_parser.add_argument("--plan-id", required=True)
    plan_parser.add_argument("--start-date", required=True)
    plan_parser.add_argument("--end-date", required=True)
    plan_parser.add_argument("--refresh-through-date")
    plan_parser.add_argument("--report-json", type=Path)
    plan_parser.add_argument("--gcs-report-uri")
    wait_parser = subparsers.add_parser("wait-for-dag-run")
    wait_parser.add_argument("--dag-id", required=True)
    wait_parser.add_argument("--run-id", required=True)
    wait_parser.add_argument("--timeout-seconds", type=int, default=DEFAULT_RUN_TIMEOUT_SECONDS)
    wait_parser.add_argument("--poll-interval-seconds", type=int, default=DEFAULT_RUN_POLL_INTERVAL_SECONDS)
    execute_parser = subparsers.add_parser("execute")
    execute_parser.add_argument("--plan-json", required=True, type=Path)
    args = parser.parse_args(argv)
    if args.mode == "wait-for-dag-run":
        wait_for_dag_run(
            args.dag_id,
            args.run_id,
            timeout_seconds=args.timeout_seconds,
            poll_interval_seconds=args.poll_interval_seconds,
        )
        return 0
    if args.mode == "execute":
        plan = json.loads(args.plan_json.read_text(encoding="utf-8"))
        if not isinstance(plan, dict):
            raise ValueError("Historical correction plan JSON must contain an object")
        execute_historical_correction_plan(plan)
        return 0
    plan = build_historical_correction_plan(
        args.start_date,
        args.end_date,
        plan_id=args.plan_id,
        refresh_through_date=args.refresh_through_date,
    )
    if args.report_json:
        write_plan_report(plan, args.report_json)
    if args.gcs_report_uri:
        upload_plan_report(plan, storage.Client(project=GCP_PROJECT), args.gcs_report_uri)
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
