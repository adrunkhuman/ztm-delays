from __future__ import annotations

from datetime import UTC, datetime
from hashlib import sha1
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    BIGQUERY_LOCATION,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    GPS_MODELS_DATE_ASSET,
    RAW_GPS_DATE_ASSET,
    dbt_command,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

try:
    from airflow.providers.standard.operators.bash import BashOperator
    from airflow.providers.standard.operators.python import PythonOperator
    from airflow.sdk import (
        DAG,
        CronPartitionTimetable,
        Metadata,
        PartitionedAssetTimetable,
        StartOfDayMapper,
        TriggerRule,
        task,
    )
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import DAG
    from airflow.operators.bash import BashOperator
    from airflow.operators.python import PythonOperator
    from airflow.sdk import (
        CronPartitionTimetable,
        Metadata,
        PartitionedAssetTimetable,
        StartOfDayMapper,
        TriggerRule,
        task,
    )

GCS_GPS_PREFIX = "raw/gps"
VEHICLE_TYPES = ("bus", "tram")

RAW_GPS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gps_pings"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
GTFS_TRIP_MATCHING_STAGING_MODELS = "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__calendar_dates"
GTFS_STOP_ARRIVAL_STAGING_MODELS = "stg_gtfs__stop_times stg_gtfs__stops"
GPS_COMPLETENESS_MODEL = "int_gps_hourly_completeness"
TRIP_SUMMARY_MODEL = "int_trip_summary"
TRIP_FACT_MODEL = "fct_trip"
STOP_ARRIVAL_FACT_MODEL = "fct_stop_arrival"
DAY_COMPLETENESS_MODEL = "mart_day_completeness"
SERVICE_COVERAGE_MODEL = "agg_service_coverage"
PIPELINE_STATUS_MODEL = "mart_pipeline_status"
AGGREGATE_MODELS = "agg_line_stop_period agg_stop_period agg_time_period agg_line_daily"
WAREHOUSE_HISTORY_START_DATE = "2026-06-25"

RAW_GPS_PROCESSING_DATE = "{{ (dag_run.partition_key or dag_run.conf.get('processing_date'))[:10] }}"
PROCESSING_DATE = "{{ dag_run.partition_key or dag_run.conf.get('processing_date') or data_interval_start.in_timezone('Europe/Warsaw').to_date_string() }}"
PRIOR_SERVICE_DATE = "{{ macros.ds_add(dag_run.partition_key or dag_run.conf.get('processing_date') or data_interval_start.in_timezone('Europe/Warsaw').to_date_string(), -1) }}"
GPS_DBT_VARS = '{"processing_date": "' + PROCESSING_DATE + '"}'
GPS_TRIP_DBT_VARS = (
    '{"processing_date": "'
    + PROCESSING_DATE
    + '", "gtfs_snapshot_id": "{{ ti.xcom_pull(task_ids=\'selected_gtfs_snapshot_id\') }}"}'
)
FACT_CURRENT_DBT_VARS = (
    '{"processing_date": "'
    + PROCESSING_DATE
    + '", "gtfs_snapshot_id": "{{ ti.xcom_pull(task_ids=\'selected_gtfs_snapshot_id\') }}", '
    + '"publish_service_date": "'
    + PROCESSING_DATE
    + '"}'
)
FACT_PRIOR_DBT_VARS = (
    '{"processing_date": "'
    + PROCESSING_DATE
    + '", "gtfs_snapshot_id": "{{ ti.xcom_pull(task_ids=\'selected_gtfs_snapshot_id\') }}", '
    + '"publish_service_date": "'
    + PRIOR_SERVICE_DATE
    + '", "aggregation_start_date": "'
    + PRIOR_SERVICE_DATE
    + '"}'
)
MART_DBT_VARS = (
    '{"processing_date": "'
    + PROCESSING_DATE
    + '", "gtfs_snapshot_id": "{{ ti.xcom_pull(task_ids=\'selected_gtfs_snapshot_id\') }}", '
    + '"aggregation_start_date": "'
    + WAREHOUSE_HISTORY_START_DATE
    + '"}'
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


def _load_raw_gps_pings(processing_date: str) -> int:
    uris = _available_gps_part_uris(processing_date)
    if not uris:
        return 0

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
    return len(uris)


def _selected_gtfs_snapshot_id(processing_date: str) -> str:
    client = bigquery.Client(project=GCP_PROJECT)
    # A snapshot first governs the Warsaw service date after its local snapshot date.
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
    dag_id="dag_gps_raw_load",
    description="Load available GPS parts hourly and emit a partitioned raw GPS date asset.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronPartitionTimetable("20 * * * *", timezone="Europe/Warsaw"),
    catchup=False,
    tags=["ztm", "gps"],
) as raw_gps_dag:

    @task(outlets=[RAW_GPS_DATE_ASSET])
    def load_raw_gps_pings(processing_date: str) -> Iterator[Metadata]:
        """Load raw GPS files and emit the processed date partition."""
        loaded_uri_count = _load_raw_gps_pings(processing_date)
        yield Metadata(
            RAW_GPS_DATE_ASSET,
            {"processing_date": processing_date, "loaded_uri_count": loaded_uri_count},
        )

    load_raw_gps_pings(RAW_GPS_PROCESSING_DATE)


with DAG(
    dag_id="dag_daily_gps",
    description="Consume raw GPS date assets and rebuild warehouse models for one processing date.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=PartitionedAssetTimetable(assets=RAW_GPS_DATE_ASSET, default_partition_mapper=StartOfDayMapper()),
    catchup=False,
    max_active_runs=1,
    tags=["ztm", "gps", "warehouse"],
) as dag:
    selected_gtfs_snapshot_id = PythonOperator(
        task_id="selected_gtfs_snapshot_id",
        python_callable=_selected_gtfs_snapshot_id,
        op_kwargs={"processing_date": PROCESSING_DATE},
    )

    dbt_run_stg_gps_pings = BashOperator(
        task_id="dbt_run_stg_gps_pings",
        bash_command=dbt_command("run", "stg_gps__pings", GPS_DBT_VARS),
    )

    dbt_run_int_ping_trip = BashOperator(
        task_id="dbt_run_int_ping_trip",
        bash_command=dbt_command("run", f"{GTFS_TRIP_MATCHING_STAGING_MODELS} int_ping_trip", GPS_TRIP_DBT_VARS),
    )

    dbt_run_int_gps_hourly_completeness = BashOperator(
        task_id="dbt_run_int_gps_hourly_completeness",
        bash_command=dbt_command("run", GPS_COMPLETENESS_MODEL, GPS_DBT_VARS),
    )

    dbt_test_stg_gps_pings = BashOperator(
        task_id="dbt_test_stg_gps_pings",
        bash_command=dbt_command("test", "source:raw.raw_gps_pings stg_gps__pings", GPS_DBT_VARS),
    )

    dbt_test_int_ping_trip = BashOperator(
        task_id="dbt_test_int_ping_trip",
        bash_command=dbt_command("test", "int_ping_trip", GPS_TRIP_DBT_VARS),
    )

    dbt_test_int_gps_hourly_completeness = BashOperator(
        task_id="dbt_test_int_gps_hourly_completeness",
        bash_command=dbt_command("test", GPS_COMPLETENESS_MODEL, GPS_DBT_VARS),
    )

    dbt_run_int_stop_arrivals = BashOperator(
        task_id="dbt_run_int_stop_arrivals",
        bash_command=dbt_command("run", f"{GTFS_STOP_ARRIVAL_STAGING_MODELS} int_stop_arrivals", GPS_TRIP_DBT_VARS),
    )

    dbt_test_int_stop_arrivals = BashOperator(
        task_id="dbt_test_int_stop_arrivals",
        bash_command=dbt_command("test", "int_stop_arrivals", GPS_TRIP_DBT_VARS),
    )

    dbt_run_int_trip_summary = BashOperator(
        task_id="dbt_run_int_trip_summary",
        bash_command=dbt_command("run", TRIP_SUMMARY_MODEL, GPS_TRIP_DBT_VARS),
    )

    dbt_test_int_trip_summary = BashOperator(
        task_id="dbt_test_int_trip_summary",
        bash_command=dbt_command("test", TRIP_SUMMARY_MODEL, GPS_TRIP_DBT_VARS),
    )

    dbt_run_fct_trip_current = BashOperator(
        task_id="dbt_run_fct_trip_current",
        bash_command=dbt_command("run", TRIP_FACT_MODEL, FACT_CURRENT_DBT_VARS),
    )

    dbt_test_fct_trip_current = BashOperator(
        task_id="dbt_test_fct_trip_current",
        bash_command=dbt_command("test", TRIP_FACT_MODEL, FACT_CURRENT_DBT_VARS),
    )

    dbt_run_fct_stop_arrival_current = BashOperator(
        task_id="dbt_run_fct_stop_arrival_current",
        bash_command=dbt_command("run", STOP_ARRIVAL_FACT_MODEL, FACT_CURRENT_DBT_VARS),
    )

    dbt_test_fct_stop_arrival_current = BashOperator(
        task_id="dbt_test_fct_stop_arrival_current",
        bash_command=dbt_command(
            "test", STOP_ARRIVAL_FACT_MODEL, FACT_CURRENT_DBT_VARS, "--indirect-selection cautious"
        ),
    )

    dbt_run_fct_trip_prior = BashOperator(
        task_id="dbt_run_fct_trip_prior",
        bash_command=dbt_command("run", TRIP_FACT_MODEL, FACT_PRIOR_DBT_VARS),
    )

    dbt_test_fct_trip_prior = BashOperator(
        task_id="dbt_test_fct_trip_prior",
        bash_command=dbt_command("test", TRIP_FACT_MODEL, FACT_PRIOR_DBT_VARS),
    )

    dbt_run_fct_stop_arrival_prior = BashOperator(
        task_id="dbt_run_fct_stop_arrival_prior",
        bash_command=dbt_command("run", STOP_ARRIVAL_FACT_MODEL, FACT_PRIOR_DBT_VARS),
    )

    dbt_test_fct_stop_arrival_prior = BashOperator(
        task_id="dbt_test_fct_stop_arrival_prior",
        bash_command=dbt_command("test", STOP_ARRIVAL_FACT_MODEL, FACT_PRIOR_DBT_VARS, "--indirect-selection cautious"),
    )

    dbt_run_completeness_and_coverage = BashOperator(
        task_id="dbt_run_completeness_and_coverage",
        bash_command=dbt_command("run", f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}", MART_DBT_VARS),
    )

    dbt_test_completeness_and_coverage = BashOperator(
        task_id="dbt_test_completeness_and_coverage",
        bash_command=dbt_command("test", f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}", MART_DBT_VARS),
    )

    dbt_run_aggregate_marts = BashOperator(
        task_id="dbt_run_aggregate_marts",
        bash_command=dbt_command("run", AGGREGATE_MODELS, MART_DBT_VARS),
    )

    dbt_test_aggregate_marts = BashOperator(
        task_id="dbt_test_aggregate_marts",
        bash_command=dbt_command("test", AGGREGATE_MODELS, MART_DBT_VARS),
    )

    dbt_run_pipeline_status = BashOperator(
        task_id="dbt_run_pipeline_status",
        bash_command=dbt_command("run", PIPELINE_STATUS_MODEL, MART_DBT_VARS),
    )

    dbt_test_pipeline_status = BashOperator(
        task_id="dbt_test_pipeline_status",
        bash_command=dbt_command("test", PIPELINE_STATUS_MODEL, MART_DBT_VARS),
    )

    @task(outlets=[GPS_MODELS_DATE_ASSET])
    def emit_gps_models_date_asset(processing_date: str) -> Iterator[Metadata]:
        """Emit the completed GPS warehouse partition."""
        yield Metadata(GPS_MODELS_DATE_ASSET, {"processing_date": processing_date})

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def fail_on_any_task_failure() -> None:
        """Fail the DAG run when any watched task fails."""
        raise RuntimeError("dag_daily_gps failed because one or more upstream tasks failed")

    selected_gtfs_snapshot_id >> dbt_run_int_ping_trip
    dbt_run_stg_gps_pings >> dbt_test_stg_gps_pings
    dbt_test_stg_gps_pings >> dbt_run_int_ping_trip >> dbt_test_int_ping_trip >> dbt_run_int_stop_arrivals
    dbt_test_stg_gps_pings >> dbt_run_int_gps_hourly_completeness >> dbt_test_int_gps_hourly_completeness
    dbt_run_int_stop_arrivals >> dbt_test_int_stop_arrivals >> dbt_run_int_trip_summary
    dbt_run_int_trip_summary >> dbt_test_int_trip_summary
    dbt_test_int_trip_summary >> dbt_run_fct_trip_current >> dbt_test_fct_trip_current
    dbt_test_fct_trip_current >> dbt_run_fct_stop_arrival_current >> dbt_test_fct_stop_arrival_current
    dbt_test_int_trip_summary >> dbt_run_fct_trip_prior >> dbt_test_fct_trip_prior
    dbt_test_fct_trip_prior >> dbt_run_fct_stop_arrival_prior >> dbt_test_fct_stop_arrival_prior
    for upstream_task in [
        dbt_test_fct_stop_arrival_current,
        dbt_test_fct_stop_arrival_prior,
        dbt_test_int_gps_hourly_completeness,
    ]:
        upstream_task >> dbt_run_completeness_and_coverage
    dbt_run_completeness_and_coverage >> dbt_test_completeness_and_coverage >> dbt_run_aggregate_marts
    dbt_run_aggregate_marts >> dbt_test_aggregate_marts >> dbt_run_pipeline_status >> dbt_test_pipeline_status
    dbt_test_pipeline_status >> emit_gps_models_date_asset(PROCESSING_DATE)

    watcher = fail_on_any_task_failure()
    for watched_task in [
        selected_gtfs_snapshot_id,
        dbt_run_stg_gps_pings,
        dbt_test_stg_gps_pings,
        dbt_run_int_ping_trip,
        dbt_test_int_ping_trip,
        dbt_run_int_gps_hourly_completeness,
        dbt_test_int_gps_hourly_completeness,
        dbt_run_int_stop_arrivals,
        dbt_test_int_stop_arrivals,
        dbt_run_int_trip_summary,
        dbt_test_int_trip_summary,
        dbt_run_fct_trip_current,
        dbt_test_fct_trip_current,
        dbt_run_fct_stop_arrival_current,
        dbt_test_fct_stop_arrival_current,
        dbt_run_fct_trip_prior,
        dbt_test_fct_trip_prior,
        dbt_run_fct_stop_arrival_prior,
        dbt_test_fct_stop_arrival_prior,
        dbt_run_completeness_and_coverage,
        dbt_test_completeness_and_coverage,
        dbt_run_aggregate_marts,
        dbt_test_aggregate_marts,
        dbt_run_pipeline_status,
        dbt_test_pipeline_status,
    ]:
        watched_task >> watcher


if __name__ == "__main__":
    raw_gps_dag.test()
    dag.test()
