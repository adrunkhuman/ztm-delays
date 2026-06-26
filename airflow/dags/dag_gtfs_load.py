from __future__ import annotations

import csv
import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import Conflict, NotFound
from google.cloud import bigquery, storage

try:
    from airflow.sdk import DAG, task
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import DAG
    from airflow.decorators import task

GCP_PROJECT = "ztm-data"
BIGQUERY_DATASET = "ztm_bq"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_snapshots"
GTFS_DATE_LENGTH = 8


@dataclass(frozen=True)
class GtfsTableSpec:
    """Raw BigQuery load contract for one GTFS text file."""

    filename: str
    table: str
    schema: list[bigquery.SchemaField]


GTFS_TABLES = (
    GtfsTableSpec(
        filename="trips.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_trips",
        schema=[
            bigquery.SchemaField("trip_id", "STRING"),
            bigquery.SchemaField("route_id", "STRING"),
            bigquery.SchemaField("service_id", "STRING"),
            bigquery.SchemaField("trip_headsign", "STRING"),
            bigquery.SchemaField("direction_id", "INTEGER"),
            bigquery.SchemaField("block_id", "STRING"),
            bigquery.SchemaField("block_short_name", "STRING"),
            bigquery.SchemaField("shape_id", "STRING"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="stop_times.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_stop_times",
        schema=[
            bigquery.SchemaField("trip_id", "STRING"),
            bigquery.SchemaField("stop_id", "STRING"),
            bigquery.SchemaField("stop_sequence", "INTEGER"),
            bigquery.SchemaField("arrival_time", "STRING"),
            bigquery.SchemaField("departure_time", "STRING"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="stops.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_stops",
        schema=[
            bigquery.SchemaField("stop_id", "STRING"),
            bigquery.SchemaField("stop_name", "STRING"),
            bigquery.SchemaField("stop_lat", "FLOAT"),
            bigquery.SchemaField("stop_lon", "FLOAT"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="shapes.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_shapes",
        schema=[
            bigquery.SchemaField("shape_id", "STRING"),
            bigquery.SchemaField("shape_pt_lat", "FLOAT"),
            bigquery.SchemaField("shape_pt_lon", "FLOAT"),
            bigquery.SchemaField("shape_pt_sequence", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="routes.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_routes",
        schema=[
            bigquery.SchemaField("route_id", "STRING"),
            bigquery.SchemaField("route_short_name", "STRING"),
            bigquery.SchemaField("route_type", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="calendar_dates.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gtfs_calendar_dates",
        schema=[
            bigquery.SchemaField("service_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("exception_type", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
)


def _latest_gtfs_snapshot() -> dict[str, str]:
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""  # noqa: S608 - table name is a module constant, not user input.
        select snapshot_id, gcs_path
        from `{RAW_GTFS_SNAPSHOTS_TABLE}`
        order by snapshot_timestamp desc
        limit 1
    """
    try:
        rows = list(client.query(query).result())
    except NotFound as exc:
        raise RuntimeError("No GTFS snapshot metadata table found") from exc
    if not rows:
        raise RuntimeError("No GTFS snapshot metadata rows found")
    return {"snapshot_id": str(rows[0].snapshot_id), "gcs_path": str(rows[0].gcs_path)}


def _parse_gcs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Expected GCS URI, got {uri}")
    bucket_name, blob_name = uri.removeprefix("gs://").split("/", 1)
    return bucket_name, blob_name


def _download_snapshot_zip(gcs_path: str) -> bytes:
    bucket_name, blob_name = _parse_gcs_uri(gcs_path)
    return storage.Client(project=GCP_PROJECT).bucket(bucket_name).blob(blob_name).download_as_bytes()


def _extract_gtfs_table(zip_file: zipfile.ZipFile, spec: GtfsTableSpec, snapshot_id: str, output_dir: Path) -> Path:
    if spec.filename not in zip_file.namelist():
        raise RuntimeError(f"GTFS snapshot missing required file: {spec.filename}")

    output_path = output_dir / spec.filename.replace(".txt", ".csv")
    fieldnames = [field.name for field in spec.schema]

    with (
        zip_file.open(spec.filename) as source_file,
        output_path.open("w", newline="", encoding="utf-8") as output_file,
    ):
        reader = csv.DictReader(line.decode("utf-8-sig") for line in source_file)
        writer = csv.DictWriter(output_file, fieldnames=fieldnames, extrasaction="ignore", lineterminator="\n")
        writer.writeheader()
        for row in reader:
            writer.writerow(
                {field.name: _csv_value(row.get(field.name, ""), field) for field in spec.schema[:-1]}
                | {"gtfs_snapshot_id": snapshot_id}
            )

    return output_path


def _csv_value(value: str, field: bigquery.SchemaField) -> str:
    if field.field_type == "DATE" and len(value) == GTFS_DATE_LENGTH and value.isdigit():
        return f"{value[:4]}-{value[4:6]}-{value[6:]}"
    return value


def _load_csv_to_bigquery(client: bigquery.Client, csv_path: Path, spec: GtfsTableSpec, snapshot_id: str) -> None:
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        schema=spec.schema,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    job_id = f"load_{spec.table.rsplit('.', 1)[-1]}_{_bigquery_job_id_suffix(snapshot_id)}"
    with csv_path.open("rb") as csv_file:
        try:
            job = client.load_table_from_file(csv_file, spec.table, job_config=job_config, job_id=job_id)
        except Conflict:
            job = client.get_job(job_id, project=GCP_PROJECT)
    job.result()


def _bigquery_job_id_suffix(value: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", value).strip("_")


def _load_gtfs_snapshot(snapshot: dict[str, str]) -> None:
    zip_bytes = _download_snapshot_zip(snapshot["gcs_path"])
    bigquery_client = bigquery.Client(project=GCP_PROJECT)

    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir)
        zip_path = temp_path / "snapshot.zip"
        zip_path.write_bytes(zip_bytes)
        with zipfile.ZipFile(zip_path) as zip_file:
            for spec in GTFS_TABLES:
                csv_path = _extract_gtfs_table(zip_file, spec, snapshot["snapshot_id"], temp_path)
                _load_csv_to_bigquery(bigquery_client, csv_path, spec, snapshot["snapshot_id"])


with DAG(
    dag_id="dag_gtfs_load",
    description="Load latest GTFS snapshot ZIP into raw BigQuery tables.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "gtfs"],
) as dag:

    @task
    def latest_gtfs_snapshot() -> dict[str, str]:
        """Return the latest GTFS snapshot metadata row."""
        return _latest_gtfs_snapshot()

    @task
    def load_gtfs_snapshot(snapshot: dict[str, str]) -> None:
        """Load selected GTFS text files from one snapshot ZIP into raw BigQuery tables."""
        _load_gtfs_snapshot(snapshot)

    load_gtfs_snapshot(latest_gtfs_snapshot())
