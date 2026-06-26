from __future__ import annotations

import hashlib
import zipfile
from datetime import UTC, datetime, timedelta
from io import BytesIO
from zoneinfo import ZoneInfo

import requests
from google.api_core.exceptions import NotFound
from google.cloud import bigquery, storage

try:
    from airflow.providers.standard.operators.empty import EmptyOperator
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import DAG
    from airflow.decorators import task
    from airflow.operators.empty import EmptyOperator
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

GCP_PROJECT = "ztm-data"
BIGQUERY_DATASET = "ztm_bq"
GCS_BUCKET = "ztm-analytics-bucket"
GTFS_URL = "https://mkuran.pl/gtfs/warsaw.zip"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_snapshots"
WARSAW_TZ = ZoneInfo("Europe/Warsaw")


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _snapshot_id(snapshot_timestamp: str, file_hash: str) -> str:
    return f"{snapshot_timestamp}_{file_hash[:12]}"


def _gtfs_gcs_path(snapshot_timestamp: str) -> str:
    return f"raw/gtfs/{snapshot_timestamp}.zip"


def _gtfs_gcs_uri(snapshot_timestamp: str) -> str:
    return f"gs://{GCS_BUCKET}/{_gtfs_gcs_path(snapshot_timestamp)}"


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
    query = f"""  # noqa: S608 - table name is a module constant, not user input.
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


def _upload_gtfs_zip(snapshot_timestamp: str, zip_bytes: bytes) -> str:
    bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    gcs_path = _gtfs_gcs_path(snapshot_timestamp)
    bucket.blob(gcs_path).upload_from_string(zip_bytes, content_type="application/zip")
    return f"gs://{GCS_BUCKET}/{gcs_path}"


def _insert_gtfs_snapshot(client: bigquery.Client, snapshot_timestamp: str, file_hash: str, gcs_path: str) -> str:
    snapshot_id = _snapshot_id(snapshot_timestamp, file_hash)
    row = {
        "snapshot_id": snapshot_id,
        "snapshot_timestamp": snapshot_timestamp,
        "file_hash": file_hash,
        "gcs_path": gcs_path,
    }
    errors = client.insert_rows_json(RAW_GTFS_SNAPSHOTS_TABLE, [row])
    if errors:
        raise RuntimeError(f"Failed to insert GTFS snapshot metadata: {errors}")
    return snapshot_id


def _gtfs_staging_processing_date(snapshot_timestamp: str) -> str:
    snapshot_datetime = datetime.fromisoformat(snapshot_timestamp)
    return (snapshot_datetime.astimezone(WARSAW_TZ).date() + timedelta(days=1)).isoformat()


def _poll_gtfs_snapshot() -> dict[str, str]:
    zip_bytes = _download_gtfs_zip()
    file_hash = _sha256(zip_bytes)
    bigquery_client = bigquery.Client(project=GCP_PROJECT)
    _ensure_raw_gtfs_snapshots_table(bigquery_client)

    if _latest_gtfs_hash(bigquery_client) == file_hash:
        return {"status": "unchanged"}

    snapshot_timestamp = datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    gcs_path = _upload_gtfs_zip(snapshot_timestamp, zip_bytes)
    snapshot_id = _insert_gtfs_snapshot(bigquery_client, snapshot_timestamp, file_hash, gcs_path)
    return {
        "status": "uploaded",
        "snapshot_id": snapshot_id,
        "gcs_path": gcs_path,
        "processing_date": _gtfs_staging_processing_date(snapshot_timestamp),
    }


def _gtfs_load_branch(poll_result: dict[str, str]) -> str:
    if poll_result["status"] == "uploaded":
        return "trigger_gtfs_load"
    if poll_result["status"] == "unchanged":
        return "skip_gtfs_load"
    raise ValueError(f"Unexpected GTFS poll result: {poll_result['status']}")


with DAG(
    dag_id="dag_gtfs_poll",
    description="Download GTFS ZIP when changed and record snapshot metadata.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "gtfs"],
) as dag:

    @task
    def poll_gtfs_snapshot() -> dict[str, str]:
        """Poll GTFS source and persist a new snapshot only when content changes."""
        return _poll_gtfs_snapshot()

    @task.branch
    def branch_gtfs_load(poll_result: dict[str, str]) -> str:
        """Route changed snapshots to raw loading and skip unchanged snapshots."""
        return _gtfs_load_branch(poll_result)

    trigger_gtfs_load = TriggerDagRunOperator(
        task_id="trigger_gtfs_load",
        trigger_dag_id="dag_gtfs_load",
        conf={
            "snapshot_id": "{{ ti.xcom_pull(task_ids='poll_gtfs_snapshot')['snapshot_id'] }}",
            "gcs_path": "{{ ti.xcom_pull(task_ids='poll_gtfs_snapshot')['gcs_path'] }}",
            "processing_date": "{{ ti.xcom_pull(task_ids='poll_gtfs_snapshot')['processing_date'] }}",
        },
        wait_for_completion=False,
    )
    skip_gtfs_load = EmptyOperator(task_id="skip_gtfs_load")

    poll_result = poll_gtfs_snapshot()
    branch_gtfs_load(poll_result) >> [trigger_gtfs_load, skip_gtfs_load]
