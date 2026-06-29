from __future__ import annotations

import csv
import re
import tempfile
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.sdk import DAG, get_current_context, task
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import DAG
    from airflow.decorators import task
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import get_current_context

GCP_PROJECT = "ztm-data"
BIGQUERY_RAW_DATASET = "ztm_raw"
BIGQUERY_LOCATION = "europe-north1"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
GTFS_DATE_LENGTH = 8
DBT_PROJECT_DIR = "/opt/airflow/dbt"
GTFS_STAGING_MODELS = (
    "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__stops stg_gtfs__shapes stg_gtfs__routes stg_gtfs__calendar_dates"
)
GTFS_DIMENSION_MODELS = "dim_line dim_stop_post dim_stop_group dim_date"
GTFS_RAW_SOURCES = (
    "source:raw.raw_gtfs_snapshots "
    "source:raw.raw_gtfs_trips "
    "source:raw.raw_gtfs_stop_times "
    "source:raw.raw_gtfs_stops "
    "source:raw.raw_gtfs_shapes "
    "source:raw.raw_gtfs_routes "
    "source:raw.raw_gtfs_calendar_dates"
)
GTFS_STAGING_PROCESSING_DATE = "{{ dag_run.conf['processing_date'] }}"
GTFS_SNAPSHOT_ID = "{{ dag_run.conf['snapshot_id'] }}"
GTFS_DBT_VARS = f'{{"processing_date": "{GTFS_STAGING_PROCESSING_DATE}", "gtfs_snapshot_id": "{GTFS_SNAPSHOT_ID}"}}'


@dataclass(frozen=True)
class GtfsTableSpec:
    """Raw BigQuery load contract for one GTFS text file."""

    filename: str
    table: str
    schema: list[bigquery.SchemaField]
    clustering_fields: list[str] | None = None


GTFS_TABLES = (
    GtfsTableSpec(
        filename="trips.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_trips",
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
        clustering_fields=["gtfs_snapshot_id"],
    ),
    GtfsTableSpec(
        filename="stop_times.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_stop_times",
        schema=[
            bigquery.SchemaField("trip_id", "STRING"),
            bigquery.SchemaField("stop_id", "STRING"),
            bigquery.SchemaField("stop_sequence", "INTEGER"),
            bigquery.SchemaField("arrival_time", "STRING"),
            bigquery.SchemaField("departure_time", "STRING"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
        clustering_fields=["gtfs_snapshot_id"],
    ),
    GtfsTableSpec(
        filename="stops.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_stops",
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
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_shapes",
        schema=[
            bigquery.SchemaField("shape_id", "STRING"),
            bigquery.SchemaField("shape_pt_lat", "FLOAT"),
            bigquery.SchemaField("shape_pt_lon", "FLOAT"),
            bigquery.SchemaField("shape_pt_sequence", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
        clustering_fields=["gtfs_snapshot_id"],
    ),
    GtfsTableSpec(
        filename="routes.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_routes",
        schema=[
            bigquery.SchemaField("route_id", "STRING"),
            bigquery.SchemaField("route_short_name", "STRING"),
            bigquery.SchemaField("route_type", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
    GtfsTableSpec(
        filename="calendar_dates.txt",
        table=f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_calendar_dates",
        schema=[
            bigquery.SchemaField("service_id", "STRING"),
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("exception_type", "INTEGER"),
            bigquery.SchemaField("gtfs_snapshot_id", "STRING", mode="REQUIRED"),
        ],
    ),
)


def _selected_gtfs_snapshot(dag_run: object) -> dict[str, str]:
    conf = getattr(dag_run, "conf", None)
    if not isinstance(conf, dict):
        raise TypeError("dag_gtfs_load requires snapshot metadata in dag_run.conf")

    snapshot_id = conf.get("snapshot_id")
    gcs_path = conf.get("gcs_path")
    processing_date = conf.get("processing_date")
    if not all(isinstance(value, str) and value for value in (snapshot_id, gcs_path, processing_date)):
        raise RuntimeError("dag_gtfs_load requires snapshot_id, gcs_path, and processing_date in dag_run.conf")

    return {"snapshot_id": snapshot_id, "gcs_path": gcs_path}


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
        clustering_fields=spec.clustering_fields,
    )
    job_id = f"load_{spec.table.rsplit('.', 1)[-1]}_{_bigquery_job_id_suffix(snapshot_id)}"
    with csv_path.open("rb") as csv_file:
        try:
            job = client.load_table_from_file(
                csv_file,
                spec.table,
                job_config=job_config,
                job_id=job_id,
                location=BIGQUERY_LOCATION,
            )
        except Conflict:
            job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
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
    description="Load triggered GTFS snapshot ZIP into raw BigQuery tables.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "gtfs"],
) as dag:

    @task
    def selected_gtfs_snapshot() -> dict[str, str]:
        """TaskFlow boundary for immutable snapshot metadata from dag_run.conf."""
        return _selected_gtfs_snapshot(get_current_context()["dag_run"])

    @task
    def load_gtfs_snapshot(snapshot: dict[str, str]) -> None:
        """TaskFlow boundary for loading one immutable GTFS ZIP snapshot."""
        _load_gtfs_snapshot(snapshot)

    loaded_gtfs_snapshot = load_gtfs_snapshot(selected_gtfs_snapshot())

    dbt_run_gtfs_staging = BashOperator(
        task_id="dbt_run_gtfs_staging",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt run --select {GTFS_STAGING_MODELS} --vars '{GTFS_DBT_VARS}'"),
    )

    dbt_test_gtfs_staging = BashOperator(
        task_id="dbt_test_gtfs_staging",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --select {GTFS_RAW_SOURCES} {GTFS_STAGING_MODELS} "
            f"--vars '{GTFS_DBT_VARS}'"
        ),
    )

    dbt_run_gtfs_dimensions = BashOperator(
        task_id="dbt_run_gtfs_dimensions",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt run --select {GTFS_DIMENSION_MODELS} --vars '{GTFS_DBT_VARS}'"),
    )

    dbt_test_gtfs_dimensions = BashOperator(
        task_id="dbt_test_gtfs_dimensions",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt test --select {GTFS_DIMENSION_MODELS} --vars '{GTFS_DBT_VARS}'"),
    )

    loaded_gtfs_snapshot >> dbt_run_gtfs_staging >> dbt_test_gtfs_staging >> dbt_run_gtfs_dimensions
    dbt_run_gtfs_dimensions >> dbt_test_gtfs_dimensions
