from __future__ import annotations

import re
from datetime import UTC, date, datetime
from typing import cast

from airflow.exceptions import AirflowException
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sdk import DAG, TriggerRule, get_current_context, task
from google.cloud import bigquery
from ztm_airflow_common import BIGQUERY_INT_DATASET, GCP_PROJECT, airflow_failure_alert

MAX_REFRESH_DAYS = 32
DEFAULT_MAX_QUERY_BYTES = 5 * 1024**3
SNAPSHOT_PATTERN = re.compile(r"[A-Za-z0-9_.:+-]{1,160}")
PLAN_ID_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}")
PROCESSING_SNAPSHOT_TABLE = f"{GCP_PROJECT}.{BIGQUERY_INT_DATASET}.int_gtfs_processing_snapshot"
SERVING_UNIVERSE_MODELS = "int_serving_trip_stop_profile int_serving_trip_universe"
SERVING_PARTITION_MODELS = (
    "int_serving_trip_execution int_serving_stop_arrival int_serving_observed_date dim_serving_window_date "
    "int_serving_entity_window_summary mart_mode_window_summary mart_entity_daily_summary "
    "mart_entity_window_daily_summary mart_line_window_summary mart_stop_group_window_summary "
    "mart_stop_post_window_summary mart_hour_window_summary mart_entity_rankings mart_entity_timeline_daily "
    "mart_worst_delay_event mart_line_reliability_daily mart_trip_daily mart_trip_mode_daily_summary "
    "mart_trip_line_daily mart_line_trip_group_daily mart_line_course_window mart_line_course_stop_window "
    "mart_stop_line_window_summary mart_stop_post_line_group_window mart_stop_group_line_group_window"
)
SERVING_FULL_MODELS = "dim_serving_date mart_pipeline_status_recent_summary"


def _validated_refresh_config(conf: object, run_id: str | None = None) -> dict[str, object]:  # noqa: C901
    if not isinstance(conf, dict):
        raise AirflowException("Historical serving refresh requires explicit configuration")
    days = conf.get("days")
    if not isinstance(days, list) or not 1 <= len(days) <= MAX_REFRESH_DAYS:
        raise AirflowException(f"Historical serving refresh requires 1-{MAX_REFRESH_DAYS} days")
    normalized: list[dict[str, str]] = []
    for item in days:
        if not isinstance(item, dict):
            raise AirflowException("Historical serving refresh day entries must be objects")
        processing_date = item.get("processing_date")
        snapshot_id = item.get("gtfs_snapshot_id")
        try:
            date.fromisoformat(str(processing_date))
        except ValueError as exc:
            raise AirflowException("Historical serving refresh has an invalid processing date") from exc
        if not isinstance(snapshot_id, str) or not SNAPSHOT_PATTERN.fullmatch(snapshot_id):
            raise AirflowException("Historical serving refresh has an invalid snapshot ID")
        normalized.append({"processing_date": str(processing_date), "gtfs_snapshot_id": snapshot_id})
    dates = [item["processing_date"] for item in normalized]
    if dates != sorted(set(dates)):
        raise AirflowException("Historical serving refresh dates must be unique and ascending")
    maximum_bytes = conf.get("maximum_bytes_billed", DEFAULT_MAX_QUERY_BYTES)
    if type(maximum_bytes) is not int or not 1 <= maximum_bytes <= 100 * 1024**3:
        raise AirflowException("maximum_bytes_billed must be a positive integer no larger than 100 GiB")
    restore_day = conf.get("restore_day")
    if not isinstance(restore_day, dict) or restore_day not in normalized:
        raise AirflowException("Historical serving refresh requires a restore_day from the approved date set")
    plan_id = conf.get("historical_plan_id")
    if not isinstance(plan_id, str) or not PLAN_ID_PATTERN.fullmatch(plan_id):
        raise AirflowException("Historical serving refresh requires a valid historical_plan_id")
    if run_id is not None and run_id != f"matcher-historical-serving-refresh__{plan_id}":
        raise AirflowException("Historical serving refresh run ID does not match its plan")
    return {
        "days": normalized,
        "restore_day": restore_day,
        "maximum_bytes_billed": maximum_bytes,
        "historical_plan_id": plan_id,
    }


def _validate_snapshot_mappings(days: list[dict[str, str]]) -> None:
    query = f"""
        select processing_date, gtfs_snapshot_id
        from `{PROCESSING_SNAPSHOT_TABLE}`
        where processing_date in unnest(@processing_dates)
    """  # noqa: S608
    config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("processing_dates", "DATE", [item["processing_date"] for item in days])
        ],
        maximum_bytes_billed=100 * 1024**2,
    )
    rows = bigquery.Client(project=GCP_PROJECT).query(query, job_config=config).result()
    actual = {str(row.processing_date): str(row.gtfs_snapshot_id) for row in rows}
    expected = {item["processing_date"]: item["gtfs_snapshot_id"] for item in days}
    if actual != expected:
        raise AirflowException("Historical serving refresh snapshot mappings do not match warehouse lineage")


REFRESH_COMMAND = f"""
set -euo pipefail
export DBT_BIGQUERY_MAXIMUM_BYTES_BILLED="{{{{ dag_run.conf.get('maximum_bytes_billed', {DEFAULT_MAX_QUERY_BYTES}) }}}}"
cd /opt/airflow/dbt
{{% for day in dag_run.conf['days'] %}}
dbt run --select int_gtfs_trip_schedule int_gtfs_duty_chain --vars '{{"processing_date":"{{{{ day.processing_date }}}}","gtfs_snapshot_id":"{{{{ day.gtfs_snapshot_id }}}}"}}'
dbt run --select {SERVING_UNIVERSE_MODELS} --vars '{{"processing_date":"{{{{ day.processing_date }}}}","gtfs_snapshot_id":"{{{{ day.gtfs_snapshot_id }}}}"}}'
dbt test --select {SERVING_UNIVERSE_MODELS} --indirect-selection cautious --exclude test_type:generic tag:audit test_type:unit --vars '{{"processing_date":"{{{{ day.processing_date }}}}","gtfs_snapshot_id":"{{{{ day.gtfs_snapshot_id }}}}"}}'
dbt run --select {SERVING_PARTITION_MODELS} --vars '{{"processing_date":"{{{{ day.processing_date }}}}","gtfs_snapshot_id":"{{{{ day.gtfs_snapshot_id }}}}","max_gps_date":"{{{{ dag_run.conf['days'][-1].processing_date }}}}"}}'
dbt test --select {SERVING_PARTITION_MODELS} --indirect-selection cautious --exclude test_type:generic tag:audit test_type:unit --vars '{{"processing_date":"{{{{ day.processing_date }}}}","gtfs_snapshot_id":"{{{{ day.gtfs_snapshot_id }}}}","max_gps_date":"{{{{ dag_run.conf['days'][-1].processing_date }}}}"}}'
{{% endfor %}}
dbt run --select {SERVING_FULL_MODELS} --vars '{{"processing_date":"{{{{ dag_run.conf['days'][-1].processing_date }}}}","gtfs_snapshot_id":"{{{{ dag_run.conf['days'][-1].gtfs_snapshot_id }}}}"}}'
dbt test --select {SERVING_FULL_MODELS} --exclude tag:audit test_type:unit --vars '{{"processing_date":"{{{{ dag_run.conf['days'][-1].processing_date }}}}","gtfs_snapshot_id":"{{{{ dag_run.conf['days'][-1].gtfs_snapshot_id }}}}"}}'
""".strip()

RESTORE_COMMAND = """
set -euo pipefail
{% set config = ti.xcom_pull(task_ids='validate_refresh_config') %}
{% if config is none %}
echo 'Historical serving configuration was not validated; refusing schedule restoration'
exit 1
{% else %}
export DBT_BIGQUERY_MAXIMUM_BYTES_BILLED="{{ config.maximum_bytes_billed }}"
cd /opt/airflow/dbt
dbt run --select int_gtfs_trip_schedule int_gtfs_duty_chain --vars '{"processing_date":"{{ config.restore_day.processing_date }}","gtfs_snapshot_id":"{{ config.restore_day.gtfs_snapshot_id }}"}'
{% endif %}
""".strip()


with DAG(
    dag_id="dag_historical_serving_refresh",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    render_template_as_native_obj=True,
    default_args={"retries": 0},
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "dbt", "historical", "manual"],
) as dag:

    @task
    def validate_refresh_config() -> dict[str, object]:
        """Reject malformed or unbounded refresh requests before dbt runs."""
        dag_run = get_current_context().get("dag_run")
        config = _validated_refresh_config(dag_run.conf, dag_run.run_id)
        _validate_snapshot_mappings(cast("list[dict[str, str]]", config["days"]))
        return config

    refresh_serving = BashOperator(task_id="refresh_serving", bash_command=REFRESH_COMMAND)
    restore_current_schedule = BashOperator(
        task_id="restore_current_schedule", bash_command=RESTORE_COMMAND, trigger_rule=TriggerRule.ALL_DONE
    )

    trigger_serving_export = TriggerDagRunOperator(
        task_id="trigger_serving_export",
        trigger_dag_id="dag_serving_export",
        trigger_run_id="historical-serving-export__{{ dag_run.conf['historical_plan_id'] }}",
        conf={
            "export_id": "{{ dag_run.conf['historical_plan_id'] }}",
            "changed_partition_dates": "{{ dag_run.conf['days'] | map(attribute='processing_date') | list }}",
        },
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    validated = validate_refresh_config()
    validated >> refresh_serving >> restore_current_schedule
    [refresh_serving, restore_current_schedule] >> trigger_serving_export


if __name__ == "__main__":
    dag.test()
