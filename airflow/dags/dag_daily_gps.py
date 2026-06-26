from __future__ import annotations

from datetime import UTC, datetime

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
BIGQUERY_DATASET = "ztm_bq"
GCS_BUCKET = "ztm-analytics-bucket"
GCS_GPS_PREFIX = "raw/gps"
VEHICLE_TYPES = ("bus", "tram")
HOURS_PER_DAY = range(24)

RAW_GPS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_DATASET}.raw_gps_pings"
DBT_PROJECT_DIR = "/opt/airflow/dbt"


def _expected_gcs_prefixes(processing_date: str) -> list[str]:
    return [
        f"{GCS_GPS_PREFIX}/vehicle_type={vehicle_type}/date={processing_date}/hour={hour:02d}/"
        for vehicle_type in VEHICLE_TYPES
        for hour in HOURS_PER_DAY
    ]


def _expected_gcs_uris(processing_date: str) -> list[str]:
    return [f"gs://{GCS_BUCKET}/{prefix}part-*.parquet" for prefix in _expected_gcs_prefixes(processing_date)]


def _check_gps_files(processing_date: str) -> None:
    bucket = storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    missing_prefixes = []

    for prefix in _expected_gcs_prefixes(processing_date):
        has_part_file = any(
            blob.name.endswith(".parquet") and "/part-" in blob.name
            for blob in bucket.list_blobs(prefix=prefix, max_results=1)
        )
        if not has_part_file:
            missing_prefixes.append(f"gs://{GCS_BUCKET}/{prefix}")

    if missing_prefixes:
        raise AirflowException(f"Missing GPS part files for {processing_date}: {missing_prefixes}")


def _load_raw_gps_pings(processing_date: str) -> None:
    client = bigquery.Client(project=GCP_PROJECT)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        create_disposition=bigquery.CreateDisposition.CREATE_IF_NEEDED,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="Time",
        ),
        clustering_fields=["Lines"],
    )
    job_id = f"load_raw_gps_pings_{processing_date.replace('-', '')}"
    try:
        job = client.load_table_from_uri(
            _expected_gcs_uris(processing_date),
            RAW_GPS_TABLE,
            job_config=job_config,
            job_id=job_id,
        )
    except Conflict:
        job = client.get_job(job_id, project=GCP_PROJECT)
    job.result()


with DAG(
    dag_id="dag_daily_gps",
    description="Load GPS Parquet files to BigQuery and run GPS staging dbt model.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 5 * * *",
    catchup=False,
    tags=["ztm", "gps"],
) as dag:
    check_gps_files = PythonOperator(
        task_id="check_gps_files",
        python_callable=_check_gps_files,
        op_kwargs={"processing_date": "{{ ds }}"},
    )

    load_raw_gps_pings = PythonOperator(
        task_id="load_raw_gps_pings",
        python_callable=_load_raw_gps_pings,
        op_kwargs={"processing_date": "{{ ds }}"},
    )

    dbt_run_stg_gps_pings = BashOperator(
        task_id="dbt_run_stg_gps_pings",
        bash_command=(
            f'cd {DBT_PROJECT_DIR} && dbt run --select stg_gps_pings --vars \'{{"processing_date": "{{{{ ds }}}}"}}\''
        ),
    )

    dbt_test_stg_gps_pings = BashOperator(
        task_id="dbt_test_stg_gps_pings",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            'dbt test --select source:raw.raw_gps_pings stg_gps_pings --vars \'{"processing_date": "{{ ds }}"}\''
        ),
    )

    check_gps_files >> load_raw_gps_pings >> dbt_run_stg_gps_pings >> dbt_test_stg_gps_pings
