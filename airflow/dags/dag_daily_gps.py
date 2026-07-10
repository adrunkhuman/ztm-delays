from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime, timedelta
from hashlib import sha1
from typing import TYPE_CHECKING

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import (
    DAG,
    CronPartitionTimetable,
    Metadata,
    TaskGroup,
    TriggerRule,
    get_current_context,
    task,
)
from google.api_core.exceptions import Conflict
from google.cloud import bigquery, storage
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_INT_DATASET,
    BIGQUERY_LOCATION,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    GPS_MODELS_DATE_ASSET,
    RAW_GPS_DATE_ASSET,
    RAW_GPS_PREFIX,
    airflow_failure_alert,
    dbt_command,
    dbt_vars,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)

VEHICLE_TYPES = ("bus", "tram")
GPS_RAW_LOAD_CRON = "20 * * * *"
GPS_WAREHOUSE_CRON = "0 4 * * *"

RAW_GPS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gps_pings"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"
INT_GTFS_PROCESSING_SNAPSHOT_TABLE = f"{GCP_PROJECT}.{BIGQUERY_INT_DATASET}.int_gtfs_processing_snapshot"
DIM_SCHEDULE_DATE_TABLE = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.dim_schedule_date"
GTFS_TRIP_MATCHING_STAGING_MODELS = (
    "stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__stops stg_gtfs__routes stg_gtfs__calendar_dates"
)
TRIP_MATCHING_SCHEDULE_MODELS = (
    "int_gtfs_processing_snapshot int_gtfs_trip_schedule_history "
    "int_gtfs_trip_schedule int_gtfs_duty_chain int_schedule_version dim_schedule_version"
)
GTFS_STOP_ARRIVAL_STAGING_MODELS = "stg_gtfs__stop_times stg_gtfs__stops"
GPS_COMPLETENESS_MODEL = "int_gps_hourly_completeness"
TRIP_SUMMARY_MODEL = "int_trip_summary"
TRIP_FACT_MODEL = "fct_trip"
STOP_ARRIVAL_FACT_MODEL = "fct_stop_arrival"
EXPECTED_STOP_EVENT_FACT_MODEL = "fct_expected_stop_event"
DAY_COMPLETENESS_MODEL = "mart_day_completeness"
SERVICE_COVERAGE_MODEL = "agg_service_coverage"
PIPELINE_STATUS_MODEL = "mart_pipeline_status"
SERVING_UNIVERSE_MODEL = "int_serving_trip_universe"
SERVING_TRIP_EXECUTION_MODEL = "int_serving_trip_execution"
SERVING_MODELS = (
    "int_serving_trip_execution int_serving_stop_arrival "
    "dim_serving_date "
    "mart_mode_window_summary mart_entity_daily_summary mart_line_window_summary "
    "mart_stop_group_window_summary mart_stop_post_window_summary mart_hour_window_summary "
    "mart_entity_rankings mart_entity_timeline_daily mart_worst_delay_event "
    "mart_line_reliability_daily mart_trip_daily mart_trip_mode_daily_summary "
    "mart_trip_line_daily mart_line_trip_group_daily mart_line_course_window "
    "mart_line_course_stop_window mart_stop_line_window_summary "
    "mart_stop_post_line_group_window mart_stop_group_line_group_window "
    "mart_pipeline_status_recent_summary rpt_schedule_day_mapping_evidence rpt_ranking_universe_evidence"
)
PRIOR_SERVING_MODELS = (
    "int_serving_trip_execution int_serving_stop_arrival "
    "mart_mode_window_summary mart_entity_daily_summary mart_line_window_summary "
    "mart_stop_group_window_summary mart_stop_post_window_summary mart_hour_window_summary "
    "mart_entity_rankings mart_entity_timeline_daily mart_worst_delay_event "
    "mart_line_reliability_daily mart_trip_daily mart_trip_mode_daily_summary "
    "mart_trip_line_daily mart_line_trip_group_daily mart_line_course_window "
    "mart_line_course_stop_window mart_stop_line_window_summary "
    "mart_stop_post_line_group_window mart_stop_group_line_group_window"
)
WAREHOUSE_HISTORY_START_DATE = "2026-06-25"
BIGQUERY_DBT_COST_LOOKBACK_HOURS = 12
BIGQUERY_DBT_BYTES_BILLED_WARN_THRESHOLD = 100 * 1024**3
LOG_BIGQUERY_DBT_JOB_COSTS = os.getenv("LOG_BIGQUERY_DBT_JOB_COSTS", "false").lower() == "true"

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
)
PRIOR_MART_DBT_VARS = dbt_vars(
    processing_date=PRIOR_SERVICE_DATE,
    gtfs_snapshot_id=SELECTED_GTFS_SNAPSHOT_ID,
    max_gps_date=PROCESSING_DATE,
)


def _gps_date_prefixes(processing_date: str) -> list[str]:
    return [f"{RAW_GPS_PREFIX}/vehicle_type={vehicle_type}/date={processing_date}/" for vehicle_type in VEHICLE_TYPES]


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
        from `{INT_GTFS_PROCESSING_SNAPSHOT_TABLE}`
        where processing_date = @processing_date
        limit 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise AirflowException(f"No governing GTFS snapshot mapping exists for GPS processing date {processing_date}")
    return str(rows[0].gtfs_snapshot_id)


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


def _dbt_task(task_id: str, command: str, selector: str, vars_json: str, extra_args: str = "") -> BashOperator:
    return BashOperator(task_id=task_id, bash_command=dbt_command(command, selector, vars_json, extra_args))


def _dbt_run_test_pair(
    task_name: str,
    run_selector: str,
    test_selector: str,
    vars_json: str,
    test_extra_args: str = "",
) -> tuple[BashOperator, BashOperator]:
    run_task = _dbt_task(f"dbt_run_{task_name}", "run", run_selector, vars_json)
    audit_excluded_args = f"{test_extra_args} --exclude tag:audit".strip()
    test_task = _dbt_task(f"dbt_test_{task_name}", "test", test_selector, vars_json, audit_excluded_args)
    run_task >> test_task
    return run_task, test_task


with DAG(
    dag_id="dag_gps_raw_load",
    dag_display_name="GPS raw ingest",
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
    dag_display_name="GPS nightly warehouse",
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
    with TaskGroup("snapshot_lookup", group_display_name="Snapshot lookup", prefix_group_id=False) as snapshot_group:

        @task
        def selected_gtfs_snapshot_id(processing_date: str) -> str:
            """Return the latest dimension-built GTFS snapshot available at rebuild time."""
            return _selected_gtfs_snapshot_id(processing_date)

        selected_gtfs_snapshot = selected_gtfs_snapshot_id(PROCESSING_DATE)

    with TaskGroup("staging", group_display_name="Staging", prefix_group_id=False) as staging_group:
        dbt_run_stg_gps_pings, dbt_test_stg_gps_pings = _dbt_run_test_pair(
            "stg_gps_pings",
            "stg_gps__pings",
            "source:raw.raw_gps_pings stg_gps__pings",
            GPS_DBT_VARS,
            "--exclude test_type:generic",
        )

    with TaskGroup(
        "trip_reconstruction", group_display_name="Trip reconstruction", prefix_group_id=False
    ) as trip_group:
        dbt_run_int_ping_trip, dbt_test_int_ping_trip = _dbt_run_test_pair(
            "int_ping_trip",
            f"{GTFS_TRIP_MATCHING_STAGING_MODELS} {TRIP_MATCHING_SCHEDULE_MODELS} int_ping_trip",
            "int_ping_trip",
            GPS_TRIP_DBT_VARS,
        )
        dbt_run_int_stop_arrivals, dbt_test_int_stop_arrivals = _dbt_run_test_pair(
            "int_stop_arrivals",
            f"{GTFS_STOP_ARRIVAL_STAGING_MODELS} int_stop_arrivals",
            "int_stop_arrivals",
            GPS_TRIP_DBT_VARS,
        )
        dbt_run_int_trip_summary, dbt_test_int_trip_summary = _dbt_run_test_pair(
            "int_trip_summary",
            TRIP_SUMMARY_MODEL,
            TRIP_SUMMARY_MODEL,
            GPS_TRIP_DBT_VARS,
        )

    with TaskGroup("current_facts", group_display_name="Current facts", prefix_group_id=False) as current_facts_group:
        dbt_run_fct_trip_current, dbt_test_fct_trip_current = _dbt_run_test_pair(
            "fct_trip_current",
            TRIP_FACT_MODEL,
            TRIP_FACT_MODEL,
            FACT_CURRENT_DBT_VARS,
        )
        dbt_run_fct_stop_arrival_current, dbt_test_fct_stop_arrival_current = _dbt_run_test_pair(
            "fct_stop_arrival_current",
            STOP_ARRIVAL_FACT_MODEL,
            STOP_ARRIVAL_FACT_MODEL,
            FACT_CURRENT_DBT_VARS,
            "--indirect-selection cautious",
        )
        dbt_run_fct_expected_stop_event_current, dbt_test_fct_expected_stop_event_current = _dbt_run_test_pair(
            "fct_expected_stop_event_current",
            EXPECTED_STOP_EVENT_FACT_MODEL,
            EXPECTED_STOP_EVENT_FACT_MODEL,
            FACT_CURRENT_DBT_VARS,
            "--indirect-selection cautious",
        )

    with TaskGroup("prior_facts", group_display_name="Prior facts", prefix_group_id=False) as prior_facts_group:
        dbt_run_fct_trip_prior, dbt_test_fct_trip_prior = _dbt_run_test_pair(
            "fct_trip_prior",
            TRIP_FACT_MODEL,
            TRIP_FACT_MODEL,
            FACT_PRIOR_DBT_VARS,
        )
        dbt_run_fct_stop_arrival_prior, dbt_test_fct_stop_arrival_prior = _dbt_run_test_pair(
            "fct_stop_arrival_prior",
            STOP_ARRIVAL_FACT_MODEL,
            STOP_ARRIVAL_FACT_MODEL,
            FACT_PRIOR_DBT_VARS,
            "--indirect-selection cautious",
        )
        dbt_run_fct_expected_stop_event_prior, dbt_test_fct_expected_stop_event_prior = _dbt_run_test_pair(
            "fct_expected_stop_event_prior",
            EXPECTED_STOP_EVENT_FACT_MODEL,
            EXPECTED_STOP_EVENT_FACT_MODEL,
            FACT_PRIOR_DBT_VARS,
            "--indirect-selection cautious",
        )

    with TaskGroup(
        "completeness_coverage",
        group_display_name="Completeness and coverage",
        prefix_group_id=False,
    ) as completeness_group:
        dbt_run_int_gps_hourly_completeness, dbt_test_int_gps_hourly_completeness = _dbt_run_test_pair(
            "int_gps_hourly_completeness",
            GPS_COMPLETENESS_MODEL,
            GPS_COMPLETENESS_MODEL,
            GPS_DBT_VARS,
            "--exclude test_type:generic",
        )
        dbt_run_completeness_and_coverage, dbt_test_completeness_and_coverage = _dbt_run_test_pair(
            "completeness_and_coverage",
            f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}",
            f"{DAY_COMPLETENESS_MODEL} {SERVICE_COVERAGE_MODEL}",
            COMPLETENESS_COVERAGE_DBT_VARS,
        )

    with TaskGroup("pipeline_status", group_display_name="Pipeline status", prefix_group_id=False) as status_group:
        dbt_run_pipeline_status, dbt_test_pipeline_status = _dbt_run_test_pair(
            "pipeline_status",
            PIPELINE_STATUS_MODEL,
            PIPELINE_STATUS_MODEL,
            COMPLETENESS_COVERAGE_DBT_VARS,
        )

    with TaskGroup("serving_marts", group_display_name="Serving marts", prefix_group_id=False) as serving_group:
        dbt_run_serving_universe, dbt_test_serving_universe = _dbt_run_test_pair(
            "serving_universe",
            SERVING_UNIVERSE_MODEL,
            SERVING_UNIVERSE_MODEL,
            MART_DBT_VARS,
            "--indirect-selection cautious --exclude test_type:generic",
        )
        dbt_run_serving_marts_prior, dbt_test_serving_marts_prior = _dbt_run_test_pair(
            "serving_marts_prior",
            PRIOR_SERVING_MODELS,
            PRIOR_SERVING_MODELS,
            PRIOR_MART_DBT_VARS,
            "--indirect-selection cautious --exclude test_type:generic",
        )
        dbt_run_serving_marts, dbt_test_serving_marts = _dbt_run_test_pair(
            "serving_marts",
            SERVING_MODELS,
            SERVING_MODELS,
            MART_DBT_VARS,
            "--indirect-selection cautious --exclude test_type:generic",
        )

    with TaskGroup("completion", group_display_name="Completion", prefix_group_id=False) as completion_group:

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
            yield Metadata(
                GPS_MODELS_DATE_ASSET,
                {
                    "processing_date": processing_date,
                    "changed_partition_dates": [
                        (date.fromisoformat(processing_date) - timedelta(days=1)).isoformat(),
                        processing_date,
                    ],
                },
            )

        @task(trigger_rule=TriggerRule.ONE_FAILED, retries=0)
        def fail_on_any_task_failure() -> None:
            """Fail the DAG run when the single-sink graph propagates an upstream failure."""
            raise RuntimeError("dag_daily_gps failed because one or more upstream tasks failed")

    selected_gtfs_snapshot >> dbt_run_int_ping_trip
    dbt_run_stg_gps_pings >> dbt_test_stg_gps_pings
    dbt_test_stg_gps_pings >> dbt_run_int_ping_trip >> dbt_test_int_ping_trip >> dbt_run_int_stop_arrivals
    dbt_test_stg_gps_pings >> dbt_run_int_gps_hourly_completeness >> dbt_test_int_gps_hourly_completeness
    dbt_run_int_stop_arrivals >> dbt_test_int_stop_arrivals >> dbt_run_int_trip_summary
    dbt_run_int_trip_summary >> dbt_test_int_trip_summary
    dbt_test_int_trip_summary >> dbt_run_fct_trip_current >> dbt_test_fct_trip_current
    dbt_test_fct_trip_current >> dbt_run_fct_stop_arrival_current >> dbt_test_fct_stop_arrival_current
    dbt_test_fct_stop_arrival_current >> dbt_run_fct_expected_stop_event_current
    dbt_run_fct_expected_stop_event_current >> dbt_test_fct_expected_stop_event_current
    dbt_test_int_trip_summary >> dbt_run_fct_trip_prior >> dbt_test_fct_trip_prior
    dbt_test_fct_trip_prior >> dbt_run_fct_stop_arrival_prior >> dbt_test_fct_stop_arrival_prior
    dbt_test_fct_stop_arrival_prior >> dbt_run_fct_expected_stop_event_prior
    dbt_run_fct_expected_stop_event_prior >> dbt_test_fct_expected_stop_event_prior
    for upstream_task in [
        dbt_test_fct_expected_stop_event_current,
        dbt_test_fct_expected_stop_event_prior,
        dbt_test_int_gps_hourly_completeness,
    ]:
        upstream_task >> dbt_run_completeness_and_coverage
    dbt_run_completeness_and_coverage >> dbt_test_completeness_and_coverage >> dbt_run_pipeline_status
    dbt_run_pipeline_status >> dbt_test_pipeline_status
    dbt_test_pipeline_status >> dbt_run_serving_universe >> dbt_test_serving_universe
    dbt_test_serving_universe >> dbt_run_serving_marts_prior >> dbt_test_serving_marts_prior
    dbt_test_serving_marts_prior >> dbt_run_serving_marts >> dbt_test_serving_marts
    gps_models_date = emit_gps_models_date_asset(PROCESSING_DATE)
    if LOG_BIGQUERY_DBT_JOB_COSTS:
        cost_summary = log_bigquery_dbt_job_costs()
        dbt_test_serving_marts >> cost_summary
    dbt_test_serving_marts >> gps_models_date
    watcher = fail_on_any_task_failure()
    dbt_test_serving_marts >> watcher


if __name__ == "__main__":
    raw_gps_dag.test()
    dag.test()
