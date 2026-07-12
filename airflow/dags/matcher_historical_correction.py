"""Read-only, manually invoked planner for bounded matcher historical corrections."""

from __future__ import annotations

import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_INT_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    RAW_GPS_PREFIX,
)

WAREHOUSE_HISTORY_START_DATE = date(2026, 6, 25)
DEGRADED_DATES = {date(2026, 7, day) for day in (5, 6, 7)}
DEFAULT_MAX_DAYS = 31
DEFAULT_MAX_QUERY_BYTES = 5 * 1024**3
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
    checksum = getattr(blob, "md5_hash", None) or getattr(blob, "crc32c", None)
    if not name or generation is None or size is None or checksum is None:
        raise RuntimeError("Historical correction inventory requires GCS generation, byte size, and hash")
    return {"name": name, "generation": str(generation), "bytes": int(size), "hash": str(checksum)}


def _snapshot_rows(client: Any, start: date, end: date, maximum_bytes_billed: int) -> dict[str, dict[str, str]]:
    query = f"""
        select mapping.processing_date, mapping.gtfs_snapshot_id, snapshots.gcs_path
        from `{PROCESSING_SNAPSHOT_TABLE}` as mapping
        inner join `{RAW_GTFS_SNAPSHOTS_TABLE}` as snapshots using (gtfs_snapshot_id)
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


def _gps_inventory(client: Any, processing_date: str) -> list[dict[str, object]]:
    bucket = client.bucket(GCS_BUCKET)
    objects = []
    for mode in ("bus", "tram"):
        prefix = f"{RAW_GPS_PREFIX}/vehicle_type={mode}/date={processing_date}/"
        objects.extend(
            _gcs_identity(blob) | {"mode": mode}
            for blob in bucket.list_blobs(prefix=prefix)
            if str(getattr(blob, "name", "")).endswith(".parquet") and "/part-" in str(getattr(blob, "name", ""))
        )
    if not objects:
        raise RuntimeError(f"GPS input inventory is missing for {processing_date}")
    return sorted(objects, key=lambda item: str(item["name"]))


def build_historical_correction_plan(
    start_date: str | None = None,
    end_date: str | None = None,
    *,
    bq_client: Any | None = None,
    storage_client: Any | None = None,
) -> dict[str, object]:
    """Build a deterministic plan without downloading, loading, publishing, or writing cloud data."""
    if start_date is None or end_date is None:
        raise ValueError("Historical correction planning requires explicit start_date and end_date")
    start, end = date.fromisoformat(start_date), date.fromisoformat(end_date)
    max_days = _positive_env("MATCHER_HISTORICAL_MAX_DAYS", DEFAULT_MAX_DAYS)
    maximum_bytes_billed = _positive_env("MATCHER_HISTORICAL_MAX_QUERY_BYTES", DEFAULT_MAX_QUERY_BYTES)
    if start > end or start < WAREHOUSE_HISTORY_START_DATE or (end - start).days + 1 > max_days:
        raise ValueError("Historical correction date range is outside its allowed bounded window")
    planned_dates = [start + timedelta(days=index) for index in range((end - start).days + 1)]
    degraded = sorted(item.isoformat() for item in set(planned_dates) & DEGRADED_DATES)
    if degraded:
        raise ValueError(f"Historical correction refuses known degraded dates: {', '.join(degraded)}")
    bq_client = bq_client or bigquery.Client(project=GCP_PROJECT)
    storage_client = storage_client or storage.Client(project=GCP_PROJECT)
    mappings = _snapshot_rows(bq_client, start, end, maximum_bytes_billed)
    missing = [item.isoformat() for item in planned_dates if item.isoformat() not in mappings]
    if missing:
        raise RuntimeError(f"No exact GTFS snapshot mapping for: {', '.join(missing)}")
    days = []
    for processing_day in planned_dates:
        processing_date = processing_day.isoformat()
        mapping = mappings[processing_date]
        gps_objects = _gps_inventory(storage_client, processing_date)
        snapshot = _snapshot_inventory(storage_client, mapping["gcs_path"])
        days.append(
            {
                "processing_date": processing_date,
                "gtfs_snapshot_id": mapping["gtfs_snapshot_id"],
                "gtfs_snapshot": snapshot,
                "gps_inventory": gps_objects,
                "affected_partitions": {
                    "current_service_date": processing_date,
                    "prior_service_date": (processing_day - timedelta(days=1)).isoformat(),
                },
            }
        )
    gps_bytes = sum(int(gps_object["bytes"]) for planned_day in days for gps_object in planned_day["gps_inventory"])
    gtfs_bytes = sum(int(planned_day["gtfs_snapshot"]["bytes"]) for planned_day in days)
    return {
        "plan_version": "matcher-historical-correction-v1",
        "read_only": True,
        "date_range": {"start_date": start.isoformat(), "end_date": end.isoformat(), "days": len(days)},
        "bounds": {
            "warehouse_history_start_date": WAREHOUSE_HISTORY_START_DATE.isoformat(),
            "max_days": max_days,
            "maximum_bytes_billed": maximum_bytes_billed,
        },
        "estimated_input_totals": {
            "gps_objects": sum(len(item["gps_inventory"]) for item in days),
            "gps_bytes": gps_bytes,
            "gtfs_bytes": gtfs_bytes,
            "total_bytes": gps_bytes + gtfs_bytes,
        },
        "days": days,
        "sequential_commands": [
            "bq query --use_legacy_sql=false --dry_run --maximum_bytes_billed=$MATCHER_HISTORICAL_MAX_QUERY_BYTES '<bounded correction query>'",
            "Review this plan, the dry-run estimate, #132 fixture results, and shadow quality_gate before any manual reconstruction.",
            "Run one approved processing date at a time; no scheduled publication or canonical partition replacement is authorized by this planner.",
        ],
        "rollback_boundary": "No canonical data is changed by planning. Any future correction must stop before its first approved partition replacement; restoring prior canonical partitions is outside this tool.",
    }


def write_plan_report(plan: dict[str, object], path: Path) -> None:
    """Write a requested local report; planning itself never writes one."""
    path.write_text(json.dumps(plan, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def upload_plan_report(plan: dict[str, object], client: Any, gcs_uri: str) -> None:
    """Upload a requested report only; this is the planner's sole optional cloud mutation."""
    parsed = urlparse(gcs_uri)
    if parsed.scheme != "gs" or not parsed.netloc or not parsed.path:
        raise ValueError("GCS report URI must be gs://bucket/object")
    client.bucket(parsed.netloc).blob(parsed.path.removeprefix("/")).upload_from_string(
        json.dumps(plan, sort_keys=True, indent=2) + "\n", content_type="application/json"
    )


def main(argv: list[str] | None = None) -> int:
    """Manual CLI entry point; it is not an Airflow task or scheduled DAG."""
    parser = argparse.ArgumentParser(prog="matcher-historical-correction-plan")
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--report-json", type=Path)
    parser.add_argument("--gcs-report-uri")
    args = parser.parse_args(argv)
    plan = build_historical_correction_plan(args.start_date, args.end_date)
    if args.report_json:
        write_plan_report(plan, args.report_json)
    if args.gcs_report_uri:
        upload_plan_report(plan, storage.Client(project=GCP_PROJECT), args.gcs_report_uri)
    print(json.dumps(plan, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
