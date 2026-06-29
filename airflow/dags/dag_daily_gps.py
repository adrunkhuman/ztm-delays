from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha1

from airflow.exceptions import AirflowException
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import DAG
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator

GCP_PROJECT = "ztm-data"
BIGQUERY_RAW_DATASET = "ztm_raw"
BIGQUERY_LOCATION = "europe-north1"
GCS_BUCKET = "ztm-analytics-bucket"
GCS_GPS_PREFIX = "raw/gps"
VEHICLE_TYPES = ("bus", "tram")

RAW_GPS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gps_pings"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
GTFS_TRIP_MATCHING_STAGING_MODELS = "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__calendar_dates"
GTFS_STOP_ARRIVAL_STAGING_MODELS = "stg_gtfs__stop_times stg_gtfs__stops"
GPS_COMPLETENESS_MODEL = "int_gps_hourly_completeness"
PROCESSING_DATE = "{{ data_interval_start.in_timezone('Europe/Warsaw').to_date_string() }}"
GPS_DBT_VARS = '{"processing_date": "' + PROCESSING_DATE + '"}'
GPS_TRIP_DBT_VARS = (
    '{"processing_date": "'
    + PROCESSING_DATE
    + '", "gtfs_snapshot_id": "{{ ti.xcom_pull(task_ids=\'selected_gtfs_snapshot_id\') }}"}'
)


def _gps_date_prefixes(processing_date: str) -> list[str]:
    return [f"{GCS_GPS_PREFIX}/vehicle_type={vehicle_type}/date={processing_date}/" for vehicle_type in VEHICLE_TYPES]


def _available_gps_part_uris(processing_date: str) -> list[str]:
    bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    uris = []
    for prefix in _gps_date_prefixes(processing_date):
        uris.extend(
            f"gs://{GCS_BUCKET}/{blob.name}"
            for blob in bucket.list_blobs(prefix=prefix)
            if blob.name.endswith(".parquet") and "/part-" in blob.name
        )
    return sorted(uris)


def _load_job_id(uri: str) -> str:
    return f"load_raw_gps_pings_{sha1(uri.encode(), usedforsecurity=False).hexdigest()}"


def _load_raw_gps_pings(processing_date: str) -> None:
    uris = _available_gps_part_uris(processing_date)
    if not uris:
        return

    client = bigquery.Client(project=GCP_PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="Time",
            require_partition_filter=True,
        ),
        clustering_fields=["Lines"],
    )
    for uri in uris:
        job_id = _load_job_id(uri)
        try:
            job = client.load_table_from_uri(
                uri,
                RAW_GPS_TABLE,
                job_config=job_config,
                job_id=job_id,
                location=BIGQUERY_LOCATION,
            )
        except Conflict:
            job = client.get_job(job_id, project=GCP_PROJECT, location=BIGQUERY_LOCATION)
        job.result()


def _selected_gtfs_snapshot_id(processing_date: str) -> str:
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        select snapshot_id
        from `{RAW_GTFS_SNAPSHOTS_TABLE}`
        where date(snapshot_timestamp, 'Europe/Warsaw') < date(@processing_date)
        order by snapshot_timestamp desc
        limit 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise AirflowException(f"No GTFS snapshot available for GPS processing date {processing_date}")
    return str(rows[0].snapshot_id)


with DAG(
    dag_id="dag_daily_gps",
    description="Load available GPS parts hourly, select the GTFS snapshot, and rebuild GPS date-partition models.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="20 * * * *",
    catchup=False,
    tags=["ztm", "gps"],
) as dag:
    load_raw_gps_pings = PythonOperator(
        task_id="load_raw_gps_pings",
        python_callable=_load_raw_gps_pings,
        op_kwargs={"processing_date": PROCESSING_DATE},
    )

    selected_gtfs_snapshot_id = PythonOperator(
        task_id="selected_gtfs_snapshot_id",
        python_callable=_selected_gtfs_snapshot_id,
        op_kwargs={"processing_date": PROCESSING_DATE},
    )

    dbt_run_stg_gps_pings = BashOperator(
        task_id="dbt_run_stg_gps_pings",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt run --select stg_gps__pings --vars '{GPS_DBT_VARS}'"),
    )

    dbt_run_int_ping_trip = BashOperator(
        task_id="dbt_run_int_ping_trip",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select {GTFS_TRIP_MATCHING_STAGING_MODELS} int_ping_trip --vars '{GPS_TRIP_DBT_VARS}'"
        ),
    )

    dbt_run_int_gps_hourly_completeness = BashOperator(
        task_id="dbt_run_int_gps_hourly_completeness",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt run --select {GPS_COMPLETENESS_MODEL} --vars '{GPS_DBT_VARS}'"),
    )

    dbt_test_stg_gps_pings = BashOperator(
        task_id="dbt_test_stg_gps_pings",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && dbt test --select source:raw.raw_gps_pings stg_gps__pings --vars '{GPS_DBT_VARS}'"
        ),
    )

    dbt_test_int_ping_trip = BashOperator(
        task_id="dbt_test_int_ping_trip",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt test --select int_ping_trip --vars '{GPS_TRIP_DBT_VARS}'"),
    )

    dbt_test_int_gps_hourly_completeness = BashOperator(
        task_id="dbt_test_int_gps_hourly_completeness",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt test --select {GPS_COMPLETENESS_MODEL} --vars '{GPS_DBT_VARS}'"),
    )

    dbt_run_int_stop_arrivals = BashOperator(
        task_id="dbt_run_int_stop_arrivals",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --select {GTFS_STOP_ARRIVAL_STAGING_MODELS} int_stop_arrivals --vars '{GPS_TRIP_DBT_VARS}'"
        ),
    )

    dbt_test_int_stop_arrivals = BashOperator(
        task_id="dbt_test_int_stop_arrivals",
        bash_command=(f"cd {DBT_PROJECT_DIR} && dbt test --select int_stop_arrivals --vars '{GPS_TRIP_DBT_VARS}'"),
    )

    load_raw_gps_pings >> dbt_run_stg_gps_pings
    selected_gtfs_snapshot_id >> dbt_run_int_ping_trip
    dbt_run_stg_gps_pings >> dbt_run_int_ping_trip >> dbt_test_int_ping_trip >> dbt_run_int_stop_arrivals
    dbt_run_stg_gps_pings >> dbt_run_int_gps_hourly_completeness >> dbt_test_int_gps_hourly_completeness
    dbt_run_int_stop_arrivals >> dbt_test_int_stop_arrivals
    dbt_run_stg_gps_pings >> dbt_test_stg_gps_pings
