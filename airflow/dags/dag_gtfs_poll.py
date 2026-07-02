from __future__ import annotations

import hashlib
import re
import zipfile
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Protocol
from zoneinfo import ZoneInfo

import requests
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.sdk import DAG, task
from google.api_core.exceptions import Conflict, NotFound, PreconditionFailed
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_LOCATION,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    GTFS_SNAPSHOT_ASSET,
    gtfs_gcs_path,
    gtfs_gcs_uri,
    gtfs_snapshot_id,
)

GTFS_URL = "https://mkuran.pl/gtfs/warsaw.zip"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
WARSAW_TZ = ZoneInfo("Europe/Warsaw")
POLL_SNAPSHOT_TIMESTAMP = "{{ data_interval_end.in_timezone('UTC').strftime('%Y-%m-%dT%H:%M:%SZ') }}"


class _AssetOutletEvent(Protocol):
    extra: dict[str, str]


class _OutletEvents(Protocol):
    def __getitem__(self, key: object) -> _AssetOutletEvent: ...


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _download_gtfs_zip() -> bytes:
    response = requests.get(GTFS_URL, timeout=60)
    response.raise_for_status()
    if not zipfile.is_zipfile(BytesIO(response.content)):
        raise RuntimeError("GTFS download did not return a valid ZIP file")
    return response.content


def _ensure_raw_gtfs_snapshots_table(client: bigquery.Client) -> None:
    table = bigquery.Table(
        RAW_GTFS_SNAPSHOTS_TABLE,
        schema=[
            bigquery.SchemaField("snapshot_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("snapshot_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("file_hash", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("gcs_path", "STRING", mode="REQUIRED"),
        ],
    )
    client.create_table(table, exists_ok=True)


def _latest_gtfs_hash(client: bigquery.Client) -> str | None:
    query = f"""
        select file_hash
        from `{RAW_GTFS_SNAPSHOTS_TABLE}`
        order by snapshot_timestamp desc
        limit 1
    """
    try:
        rows = list(client.query(query).result())
    except NotFound:
        return None
    if not rows:
        return None
    return str(rows[0].file_hash)


def _upload_gtfs_zip(snapshot_timestamp: str, file_hash: str, zip_bytes: bytes) -> str:
    bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    snapshot_id = gtfs_snapshot_id(snapshot_timestamp, file_hash)
    gcs_path = gtfs_gcs_path(snapshot_id)
    with suppress(PreconditionFailed):
        bucket.blob(gcs_path).upload_from_string(zip_bytes, content_type="application/zip", if_generation_match=0)
    return gtfs_gcs_uri(snapshot_id)


def _insert_gtfs_snapshot(client: bigquery.Client, snapshot_timestamp: str, file_hash: str, gcs_path: str) -> str:
    snapshot_id = gtfs_snapshot_id(snapshot_timestamp, file_hash)
    query = f"""
        merge `{RAW_GTFS_SNAPSHOTS_TABLE}` as target
        using (
            select
                @snapshot_id as snapshot_id,
                timestamp(@snapshot_timestamp) as snapshot_timestamp,
                @file_hash as file_hash,
                @gcs_path as gcs_path
        ) as source
        on target.snapshot_id = source.snapshot_id
        when not matched then
            insert (snapshot_id, snapshot_timestamp, file_hash, gcs_path)
            values (source.snapshot_id, source.snapshot_timestamp, source.file_hash, source.gcs_path)
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("snapshot_id", "STRING", snapshot_id),
            bigquery.ScalarQueryParameter("snapshot_timestamp", "STRING", snapshot_timestamp),
            bigquery.ScalarQueryParameter("file_hash", "STRING", file_hash),
            bigquery.ScalarQueryParameter("gcs_path", "STRING", gcs_path),
        ]
    )
    job_id = f"merge_raw_gtfs_snapshots_{_bigquery_job_id_suffix(snapshot_id)}"
    try:
        job = client.query(query, job_config=job_config, job_id=job_id, location=BIGQUERY_LOCATION)
    except Conflict:
        job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
    job.result()
    return snapshot_id


def _bigquery_job_id_suffix(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_")


def _gtfs_staging_processing_date(snapshot_timestamp: str) -> str:
    snapshot_datetime = datetime.fromisoformat(snapshot_timestamp)
    # A snapshot first governs the Warsaw service date after its local snapshot date.
    return (snapshot_datetime.astimezone(WARSAW_TZ).date() + timedelta(days=1)).isoformat()


def _poll_gtfs_snapshot(snapshot_timestamp: str) -> dict[str, str]:
    zip_bytes = _download_gtfs_zip()
    file_hash = _sha256(zip_bytes)
    bigquery_client = bigquery.Client(project=GCP_PROJECT)
    _ensure_raw_gtfs_snapshots_table(bigquery_client)

    if _latest_gtfs_hash(bigquery_client) == file_hash:
        return {"status": "unchanged"}

    gcs_path = _upload_gtfs_zip(snapshot_timestamp, file_hash, zip_bytes)
    snapshot_id = _insert_gtfs_snapshot(bigquery_client, snapshot_timestamp, file_hash, gcs_path)
    return {
        "status": "uploaded",
        "snapshot_id": snapshot_id,
        "gcs_path": gcs_path,
        "file_hash": file_hash,
        "processing_date": _gtfs_staging_processing_date(snapshot_timestamp),
    }


def _gtfs_load_branch(poll_result: dict[str, str]) -> str:
    if poll_result["status"] == "uploaded":
        return "emit_gtfs_snapshot_asset"
    if poll_result["status"] == "unchanged":
        return "skip_gtfs_load"
    raise ValueError(f"Unexpected GTFS poll result: {poll_result['status']}")


def _gtfs_snapshot_asset_extra(poll_result: dict[str, str]) -> dict[str, str]:
    return {
        "snapshot_id": poll_result["snapshot_id"],
        "gcs_path": poll_result["gcs_path"],
        "processing_date": poll_result["processing_date"],
        "file_hash": poll_result["file_hash"],
    }


with DAG(
    dag_id="dag_gtfs_poll",
    description="Download GTFS ZIP when changed and emit a GTFS snapshot asset event.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "gtfs"],
) as dag:

    @task
    def poll_gtfs_snapshot(snapshot_timestamp: str) -> dict[str, str]:
        """Persist changed GTFS content before emitting the raw-loader asset."""
        return _poll_gtfs_snapshot(snapshot_timestamp)

    @task.branch
    def branch_gtfs_load(poll_result: dict[str, str]) -> str:
        """Avoid triggering raw reloads when the GTFS ZIP hash is unchanged."""
        return _gtfs_load_branch(poll_result)

    @task(outlets=[GTFS_SNAPSHOT_ASSET])
    def emit_gtfs_snapshot_asset(poll_result: dict[str, str], outlet_events: _OutletEvents | None = None) -> None:
        """Publish the immutable snapshot context as an Airflow asset event."""
        if outlet_events is None:
            raise RuntimeError("GTFS snapshot asset emission requires Airflow outlet_events")
        outlet_events[GTFS_SNAPSHOT_ASSET].extra = _gtfs_snapshot_asset_extra(poll_result)

    skip_gtfs_load = EmptyOperator(task_id="skip_gtfs_load")

    poll_result = poll_gtfs_snapshot(POLL_SNAPSHOT_TIMESTAMP)
    branch_gtfs_load(poll_result) >> [emit_gtfs_snapshot_asset(poll_result), skip_gtfs_load]


if __name__ == "__main__":
    dag.test()
