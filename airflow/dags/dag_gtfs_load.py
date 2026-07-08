from __future__ import annotations

import csv
import re
import tempfile
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime
from pathlib import Path

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, TriggerRule, get_current_context, task
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_LOCATION,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GTFS_SNAPSHOT_ASSET,
    airflow_failure_alert,
    dbt_command,
    dbt_vars,
    gtfs_gcs_uri,
)

RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
GTFS_DATE_LENGTH = 8
DBT_PROJECT_DIR = "/opt/airflow/dbt"
GTFS_SNAPSHOT_ID_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z_[0-9a-f]{12}$")
PROCESSING_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
GTFS_STAGING_MODELS = (
    "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__stops stg_gtfs__shapes stg_gtfs__routes stg_gtfs__calendar_dates"
)
GTFS_DIMENSION_MODELS = (
    "dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date int_gtfs_trip_schedule int_gtfs_duty_chain "
    "int_schedule_version "
    "dim_schedule_version "
    "dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current"
)
GTFS_DAILY_DIMENSION_TEST_MODELS = (
    "dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date dim_schedule_version "
    "dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current"
)
GTFS_RAW_SOURCES = (
    "source:raw.raw_gtfs_snapshots "
    "source:raw.raw_gtfs_trips "
    "source:raw.raw_gtfs_stop_times "
    "source:raw.raw_gtfs_stops "
    "source:raw.raw_gtfs_shapes "
    "source:raw.raw_gtfs_routes "
    "source:raw.raw_gtfs_calendar_dates"
)
GTFS_STAGING_PROCESSING_DATE = "{{ ti.xcom_pull(task_ids='selected_gtfs_snapshot')['processing_date'] }}"
GTFS_SNAPSHOT_ID = "{{ ti.xcom_pull(task_ids='selected_gtfs_snapshot')['snapshot_id'] }}"
GTFS_DBT_VARS = dbt_vars(processing_date=GTFS_STAGING_PROCESSING_DATE, gtfs_snapshot_id=GTFS_SNAPSHOT_ID)


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
            bigquery.SchemaField("pickup_type", "INTEGER"),
            bigquery.SchemaField("drop_off_type", "INTEGER"),
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
            bigquery.SchemaField("stop_code", "STRING"),
            bigquery.SchemaField("platform_code", "STRING"),
            bigquery.SchemaField("stop_lat", "FLOAT"),
            bigquery.SchemaField("stop_lon", "FLOAT"),
            bigquery.SchemaField("location_type", "STRING"),
            bigquery.SchemaField("parent_station", "STRING"),
            bigquery.SchemaField("wheelchair_boarding", "STRING"),
            bigquery.SchemaField("zone_id", "STRING"),
            bigquery.SchemaField("stop_name_stem", "STRING"),
            bigquery.SchemaField("town_name", "STRING"),
            bigquery.SchemaField("street_name", "STRING"),
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


def _selected_gtfs_snapshot(context: Mapping[str, object]) -> dict[str, object]:
    manual_snapshot = _manual_gtfs_snapshot(context.get("dag_run"))
    if manual_snapshot is not None:
        return _snapshot_batch([manual_snapshot])

    triggering_asset_events = context.get("triggering_asset_events")
    if not isinstance(triggering_asset_events, Mapping):
        raise TypeError("dag_gtfs_load requires a GTFS snapshot asset event or explicit dag_run.conf")
    try:
        asset_events = triggering_asset_events[GTFS_SNAPSHOT_ASSET]
    except (KeyError, IndexError, TypeError) as exc:
        raise RuntimeError("dag_gtfs_load requires a GTFS snapshot asset event or explicit dag_run.conf") from exc
    if not isinstance(asset_events, Sequence) or isinstance(asset_events, (str, bytes)):
        raise TypeError("dag_gtfs_load requires a GTFS snapshot asset event or explicit dag_run.conf")

    snapshots = [_validate_snapshot_context(getattr(event, "extra", None)) for event in asset_events]
    if not snapshots:
        raise RuntimeError("dag_gtfs_load requires a GTFS snapshot asset event or explicit dag_run.conf")
    return _snapshot_batch(snapshots)


def _snapshot_batch(snapshots: list[dict[str, str]]) -> dict[str, object]:
    latest_snapshot = snapshots[-1]
    return {**latest_snapshot, "snapshots": snapshots}


def _manual_gtfs_snapshot(dag_run: object | None) -> dict[str, str] | None:
    conf = getattr(dag_run, "conf", None)
    if not conf:
        return None
    if not isinstance(conf, dict):
        raise TypeError("dag_gtfs_load manual recovery config must be a dictionary")

    return _validate_snapshot_context(conf)


def _validate_snapshot_context(raw_context: object) -> dict[str, str]:
    if not isinstance(raw_context, dict):
        raise TypeError("GTFS snapshot context must be a dictionary")
    snapshot_id = raw_context.get("snapshot_id")
    gcs_path = raw_context.get("gcs_path")
    processing_date = raw_context.get("processing_date")
    if not isinstance(snapshot_id, str) or not GTFS_SNAPSHOT_ID_PATTERN.fullmatch(snapshot_id):
        raise RuntimeError("dag_gtfs_load requires snapshot_id, gcs_path, and processing_date")
    if not isinstance(gcs_path, str):
        raise TypeError("dag_gtfs_load requires snapshot_id, gcs_path, and processing_date")
    if gcs_path not in _accepted_gtfs_gcs_uris(snapshot_id):
        raise RuntimeError("dag_gtfs_load requires snapshot_id, gcs_path, and processing_date")
    if not isinstance(processing_date, str) or not PROCESSING_DATE_PATTERN.fullmatch(processing_date):
        raise RuntimeError("dag_gtfs_load requires snapshot_id, gcs_path, and processing_date")
    date.fromisoformat(processing_date)

    return {"snapshot_id": snapshot_id, "gcs_path": gcs_path, "processing_date": processing_date}


def _accepted_gtfs_gcs_uris(snapshot_id: str) -> set[str]:
    canonical_uri = gtfs_gcs_uri(snapshot_id)
    legacy_timestamp_uri = f"{canonical_uri.rsplit('_', 1)[0]}.zip"
    return {canonical_uri, legacy_timestamp_uri}


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


def _load_gtfs_snapshot_batch(snapshot_batch: dict[str, object]) -> None:
    snapshots = snapshot_batch.get("snapshots")
    if not isinstance(snapshots, list) or not snapshots:
        raise TypeError("GTFS snapshot batch must contain at least one snapshot")
    for snapshot in snapshots:
        _load_gtfs_snapshot(_validate_snapshot_context(snapshot))


with DAG(
    dag_id="dag_gtfs_load",
    dag_display_name="GTFS snapshot load",
    description="Load GTFS snapshot asset ZIP, then rebuild GTFS staging and dimensions.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=[GTFS_SNAPSHOT_ASSET],
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "gtfs"],
) as dag:

    @task(inlets=[GTFS_SNAPSHOT_ASSET])
    def selected_gtfs_snapshot() -> dict[str, object]:
        """Read the immutable snapshot from the asset event or manual recovery config."""
        return _selected_gtfs_snapshot(get_current_context())

    @task
    def load_gtfs_snapshot(snapshot_batch: dict[str, object]) -> None:
        """Load every snapshot that triggered this run; downstream dbt vars pin the latest."""
        _load_gtfs_snapshot_batch(snapshot_batch)

    loaded_gtfs_snapshot = load_gtfs_snapshot(selected_gtfs_snapshot())

    dbt_run_gtfs_staging = BashOperator(
        task_id="dbt_run_gtfs_staging",
        bash_command=dbt_command("run", GTFS_STAGING_MODELS, GTFS_DBT_VARS),
    )

    dbt_test_gtfs_staging = BashOperator(
        task_id="dbt_test_gtfs_staging",
        bash_command=dbt_command("test", f"{GTFS_RAW_SOURCES} {GTFS_STAGING_MODELS}", GTFS_DBT_VARS),
    )

    dbt_run_gtfs_dimensions = BashOperator(
        task_id="dbt_run_gtfs_dimensions",
        bash_command=dbt_command("run", GTFS_DIMENSION_MODELS, GTFS_DBT_VARS),
    )

    dbt_test_gtfs_dimensions = BashOperator(
        task_id="dbt_test_gtfs_dimensions",
        bash_command=dbt_command(
            "test", GTFS_DAILY_DIMENSION_TEST_MODELS, GTFS_DBT_VARS, "--indirect-selection cautious"
        ),
    )

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def fail_on_any_task_failure() -> None:
        """Fail the DAG run when the single-sink graph propagates an upstream failure."""
        raise RuntimeError("dag_gtfs_load failed because one or more upstream tasks failed")

    loaded_gtfs_snapshot >> dbt_run_gtfs_staging >> dbt_test_gtfs_staging >> dbt_run_gtfs_dimensions
    dbt_run_gtfs_dimensions >> dbt_test_gtfs_dimensions
    watcher = fail_on_any_task_failure()
    dbt_test_gtfs_dimensions >> watcher


if __name__ == "__main__":
    dag.test()
