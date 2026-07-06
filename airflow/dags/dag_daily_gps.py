from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from hashlib import sha1
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import (
    DAG,
    CronPartitionTimetable,
    Metadata,
    TriggerRule,
    get_current_context,
    task,
)
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    GPS_MODELS_DATE_ASSET,
    RAW_GPS_DATE_ASSET,
    airflow_failure_alert,
    dbt_command,
    dbt_vars,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

GCS_GPS_PREFIX = "raw/gps"
VEHICLE_TYPES = ("bus", "tram")
GPS_RAW_LOAD_CRON = "20 * * * *"
GPS_WAREHOUSE_CRON = "0 4 * * *"

RAW_GPS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gps_pings"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
DIM_SCHEDULE_DATE_TABLE = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.dim_schedule_date"
DIM_SCHEDULE_VERSION_TABLE = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.dim_schedule_version"
GTFS_TRIP_MATCHING_STAGING_MODELS = "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__calendar_dates"
TRIP_MATCHING_SCHEDULE_MODELS = "int_gtfs_trip_schedule int_schedule_version"
GTFS_STOP_ARRIVAL_STAGING_MODELS = "stg_gtfs__stop_times stg_gtfs__stops"
GPS_COMPLETENESS_MODEL = "int_gps_hourly_completeness"
TRIP_SUMMARY_MODEL = "int_trip_summary"
TRIP_FACT_MODEL = "fct_trip"
STOP_ARRIVAL_FACT_MODEL = "fct_stop_arrival"
DAY_COMPLETENESS_MODEL = "mart_day_completeness"
SERVICE_COVERAGE_MODEL = "agg_service_coverage"
PIPELINE_STATUS_MODEL = "mart_pipeline_status"
DAILY_AGGREGATE_MODEL = "agg_line_daily"
PERIOD_AGGREGATE_MODELS = "agg_line_stop_period agg_stop_period agg_time_period"
WAREHOUSE_HISTORY_START_DATE = "2026-06-25"
BIGQUERY_DBT_COST_LOOKBACK_HOURS = 12
BIGQUERY_DBT_BYTES_BILLED_WARN_THRESHOLD = 100 * 1024**3

RAW_GPS_PROCESSING_DATE = "{{ (dag_run.partition_key or dag_run.conf.get('processing_date'))[:10] }}"
PROCESSING_DATE = "{{ dag_run.conf.get('processing_date') or dag_run.partition_key or data_interval_start.in_timezone('Europe/Warsaw').to_date_string() }}"
PRIOR_SERVICE_DATE = "{{ macros.ds_add(dag_run.conf.get('processing_date') or dag_run.partition_key or data_interval_start.in_timezone('Europe/Warsaw').to_date_string(), -1) }}"
SELECTED_GTFS_SNAPSHOT_ID = "{{ ti.xcom_pull(task_ids='selected_gtfs_snapshot_id') }}"
GPS_DBT_VARS = dbt_vars(processing_date=PROCESSING_DATE)
GPS_TRIP_DBT_VARS = dbt_vars(processing_date=PROCESSING_DATE, gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID)
FACT_CURRENT_DBT_VARS = dbt_vars(
    processing_date=PROCESSING_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    publish_service_date=PROCESSING_DATE,
)
FACT_PRIOR_DBT_VARS = dbt_vars(
    processing_date=PROCESSING_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    publish_service_date=PRIOR_SERVICE_DATE,
    aggregation_start_date=PRIOR_SERVICE_DATE,
)
COMPLETENESS_COVERAGE_DBT_VARS = dbt_vars(
    processing_date=PROCESSING_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    aggregation_start_date=PRIOR_SERVICE_DATE,
)
MART_DBT_VARS = dbt_vars(
    processing_date=PROCESSING_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    aggregation_start_date=WAREHOUSE_HISTORY_START_DATE,
)
PERIOD_AGGREGATE_SOURCE_START_DATE = "{{ ti.xcom_pull(task_ids='period_aggregate_window')['source_start_date'] }}"
PERIOD_AGGREGATE_PARTITION_DATES = "{{ ti.xcom_pull(task_ids='period_aggregate_window')['partition_dates'] }}"
PERIOD_AGGREGATE_DBT_VARS = dbt_vars(
    processing_date=PROCESSING_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    aggregation_start_date=PRIOR_SERVICE_DATE,
    period_source_start_date=PERIOD_AGGREGATE_SOURCE_START_DATE,
    period_partition_dates=PERIOD_AGGREGATE_PARTITION_DATES,
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
    query = f"""
        select gtfs_snapshot_id
        from `{DIM_SCHEDULE_DATE_TABLE}`
        where service_date = date(@processing_date)
        limit 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise AirflowException(f"No built GTFS schedule dimension available for GPS processing date {processing_date}")
    return str(rows[0].gtfs_snapshot_id)


def _period_aggregate_window(aggregation_start_date: str, processing_date: str) -> dict[str, str]:
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        with affected_service_dates as (
          select service_date
          from unnest(generate_date_array(date(@aggregation_start_date), date(@processing_date))) as service_date
        ),

        affected_periods as (
          select distinct date_trunc(service_date, month) as period_start_date
          from affected_service_dates

          union distinct

          select distinct valid_from_date as period_start_date
          from `{DIM_SCHEDULE_VERSION_TABLE}`
          where valid_from_date <= date(@processing_date)
            and coalesce(valid_to_date, date '9999-12-31') >= date(@aggregation_start_date)
        )

        select
          cast(min(period_start_date) as string) as source_start_date,
          string_agg(cast(period_start_date as string), '|' order by period_start_date) as partition_dates
        from affected_periods
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("aggregation_start_date", "DATE", aggregation_start_date),
            bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date),
        ]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return {"source_start_date": aggregation_start_date, "partition_dates": aggregation_start_date}
    return {
        "source_start_date": str(rows[0].source_start_date or aggregation_start_date),
        "partition_dates": str(rows[0].partition_dates or aggregation_start_date),
    }


def _bigquery_dbt_job_cost_summary(started_at: datetime) -> dict[str, object]:
    client = bigquery.Client(project=GCP_PROJECT)
    query = f"""
        select
          count(*) as job_count,
          coalesce(sum(total_bytes_processed), 0) as total_bytes_processed,
          coalesce(sum(total_bytes_billed), 0) as total_bytes_billed,
          array_agg(struct(
            creation_time,
            job_id,
            statement_type,
            coalesce(total_bytes_processed, 0) as total_bytes_processed,
            coalesce(total_bytes_billed, 0) as total_bytes_billed,
            substr(query, 1, 500) as query_prefix
          ) order by coalesce(total_bytes_billed, total_bytes_processed, 0) desc limit 10) as top_jobs
        from `region-{BIGQUERY_LOCATION}`.INFORMATION_SCHEMA.JOBS_BY_USER
        where creation_time >= @started_at
          and job_type = 'QUERY'
          and starts_with(query, '/* {{"app": "dbt"')
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("started_at", "TIMESTAMP", started_at)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        return {"job_count": 0, "total_bytes_processed": 0, "total_bytes_billed": 0, "top_jobs": []}

    row = rows[0]
    top_jobs = [dict(job.items()) if hasattr(job, "items") else dict(job) for job in row.top_jobs or []]
    return {
        "job_count": int(row.job_count or 0),
        "total_bytes_processed": int(row.total_bytes_processed or 0),
        "total_bytes_billed": int(row.total_bytes_billed or 0),
        "top_jobs": top_jobs,
    }


with DAG(
    dag_id="dag_gps_raw_load",
    description="Load available GPS parts hourly and emit a partitioned raw GPS date asset.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronPartitionTimetable(GPS_RAW_LOAD_CRON, timezone="Europe/Warsaw"),
    catchup=False,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
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
    description="Nightly warehouse rebuild for one GPS processing date.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronPartitionTimetable(
        GPS_WAREHOUSE_CRON,
        timezone="Europe/Warsaw",
        run_offset=-1,
        key_format="%Y-%m-%d",
    ),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "gps", "warehouse"],
) as dag:

    @task
    def selected_gtfs_snapshot_id(processing_date: str) -> str:
        """Return the dimension-built GTFS snapshot that governs this GPS service date."""
        return _selected_gtfs_snapshot_id(processing_date)

    selected_gtfs_snapshot = selected_gtfs_snapshot_id(PROCESSING_DATE)

    @task
    def period_aggregate_window(aggregation_start_date: str, processing_date: str) -> dict[str, str]:
        """Return the source window and target partitions for affected period aggregates."""
        return _period_aggregate_window(aggregation_start_date, processing_date)

    period_aggregate = period_aggregate_window(PRIOR_SERVICE_DATE, PROCESSING_DATE)

    dbt_run_stg_gps_pings = BashOperator(
        task_id="dbt_run_stg_gps_pings",
        bash_command=dbt_command("run", "stg_gps__pings", GPS_DBT_VARS),
    )

    dbt_run_int_ping_trip = BashOperator(
        task_id="dbt_run_int_ping_trip",
        bash_command=dbt_command(
            "run",
            f"{GTFS_TRIP_MATCHING_STAGING_MODELS} {TRIP_MATCHING_SCHEDULE_MODELS} int_ping_trip",
            GPS_TRIP_DBT_VARS,
        ),
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
        bash_command=dbt_command("run", f"{TRIP_MATCHING_SCHEDULE_MODELS} {TRIP_SUMMARY_MODEL}", GPS_TRIP_DBT_VARS),
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
        bash_command=dbt_command(
            "run", f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}", COMPLETENESS_COVERAGE_DBT_VARS
        ),
    )

    dbt_test_completeness_and_coverage = BashOperator(
        task_id="dbt_test_completeness_and_coverage",
        bash_command=dbt_command(
            "test", f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}", COMPLETENESS_COVERAGE_DBT_VARS
        ),
    )

    dbt_run_daily_aggregate_mart = BashOperator(
        task_id="dbt_run_daily_aggregate_mart",
        bash_command=dbt_command("run", DAILY_AGGREGATE_MODEL, COMPLETENESS_COVERAGE_DBT_VARS),
    )

    dbt_run_period_aggregate_marts = BashOperator(
        task_id="dbt_run_period_aggregate_marts",
        bash_command=dbt_command("run", PERIOD_AGGREGATE_MODELS, PERIOD_AGGREGATE_DBT_VARS),
    )

    dbt_run_pipeline_status = BashOperator(
        task_id="dbt_run_pipeline_status",
        bash_command=dbt_command("run", PIPELINE_STATUS_MODEL, COMPLETENESS_COVERAGE_DBT_VARS),
    )

    dbt_test_pipeline_status = BashOperator(
        task_id="dbt_test_pipeline_status",
        bash_command=dbt_command("test", PIPELINE_STATUS_MODEL, COMPLETENESS_COVERAGE_DBT_VARS),
    )

    @task(do_xcom_push=False)
    def log_bigquery_dbt_job_costs() -> dict[str, object]:
        """Log BigQuery dbt job bytes for the current DAG run without blocking publication."""
        context = get_current_context()
        dag_run = context.get("dag_run")
        started_at = getattr(dag_run, "start_date", None)
        if not isinstance(started_at, datetime):
            started_at = datetime.now(UTC) - timedelta(hours=BIGQUERY_DBT_COST_LOOKBACK_HOURS)
        elif started_at.tzinfo is None:
            started_at = started_at.replace(tzinfo=UTC)

        try:
            summary = _bigquery_dbt_job_cost_summary(started_at)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("Failed to collect BigQuery dbt cost metadata: %s", exc)
            return {"error": str(exc), "started_at": started_at.isoformat()}

        LOGGER.info(
            "BigQuery dbt cost summary since %s: jobs=%s bytes_processed=%s bytes_billed=%s top_jobs=%s",
            started_at.isoformat(),
            summary["job_count"],
            summary["total_bytes_processed"],
            summary["total_bytes_billed"],
            summary["top_jobs"],
        )
        total_bytes_billed = summary["total_bytes_billed"]
        if isinstance(total_bytes_billed, int) and total_bytes_billed > BIGQUERY_DBT_BYTES_BILLED_WARN_THRESHOLD:
            LOGGER.warning(
                "BigQuery dbt billed bytes exceeded warning threshold: billed=%s threshold=%s",
                total_bytes_billed,
                BIGQUERY_DBT_BYTES_BILLED_WARN_THRESHOLD,
            )
        return summary | {"started_at": started_at.isoformat()}

    @task(outlets=[GPS_MODELS_DATE_ASSET])
    def emit_gps_models_date_asset(processing_date: str) -> Iterator[Metadata]:
        """Emit the completed GPS warehouse partition."""
        yield Metadata(GPS_MODELS_DATE_ASSET, {"processing_date": processing_date})

    @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
    def fail_on_any_task_failure() -> None:
        """Fail the DAG run when the single-sink graph propagates an upstream failure."""
        raise RuntimeError("dag_daily_gps failed because one or more upstream tasks failed")

    selected_gtfs_snapshot >> dbt_run_int_ping_trip
    selected_gtfs_snapshot >> period_aggregate
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
    dbt_run_completeness_and_coverage >> dbt_test_completeness_and_coverage >> dbt_run_daily_aggregate_mart
    [dbt_run_daily_aggregate_mart, period_aggregate] >> dbt_run_period_aggregate_marts
    dbt_run_period_aggregate_marts >> dbt_run_pipeline_status >> dbt_test_pipeline_status
    cost_summary = log_bigquery_dbt_job_costs()
    gps_models_date = emit_gps_models_date_asset(PROCESSING_DATE)
    dbt_test_pipeline_status >> cost_summary
    dbt_test_pipeline_status >> gps_models_date
    watcher = fail_on_any_task_failure()
    dbt_test_pipeline_status >> watcher


if __name__ == "__main__":
    raw_gps_dag.test()
    dag.test()
