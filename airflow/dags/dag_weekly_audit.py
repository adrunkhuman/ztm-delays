from __future__ import annotations

from datetime import UTC, datetime

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, task
from google.cloud import bigquery
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_INT_DATASET,
    GCP_PROJECT,
    airflow_failure_alert,
    dbt_command,
    dbt_vars,
)

AUDIT_PROCESSING_DATE = "{{ ds }}"
AUDIT_GTFS_SNAPSHOT_ID = "{{ ti.xcom_pull(task_ids='selected_gtfs_snapshot_id') }}"
AUDIT_DBT_VARS = dbt_vars(processing_date=AUDIT_PROCESSING_DATE, gtfs_snapshot_id=AUDIT_GTFS_SNAPSHOT_ID)
INT_GTFS_PROCESSING_SNAPSHOT_TABLE = f"{GCP_PROJECT}.{BIGQUERY_INT_DATASET}.int_gtfs_processing_snapshot"


def _selected_gtfs_snapshot_id(processing_date: str) -> str:
    client = bigquery.Client(project=GCP_PROJECT)
    query = "\n".join(
        (
            "select gtfs_snapshot_id",
            f"from `{INT_GTFS_PROCESSING_SNAPSHOT_TABLE}`",
            "where _PARTITIONDATE = @processing_date",
            "limit 1",
        )
    )
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ScalarQueryParameter("processing_date", "DATE", processing_date)]
    )
    rows = list(client.query(query, job_config=job_config).result())
    if not rows:
        raise RuntimeError(f"No governing GTFS snapshot mapping exists for audit processing date {processing_date}")
    return str(rows[0].gtfs_snapshot_id)


with DAG(
    dag_id="dag_weekly_audit",
    dag_display_name="Weekly warehouse audits",
    description="Run expensive audit-tagged dbt tests outside normal nightly paths.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule="0 7 * * 0",
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "audit"],
) as dag:

    @task
    def selected_gtfs_snapshot_id(processing_date: str) -> str:
        """Return the governing GTFS snapshot for the audit processing date."""
        return _selected_gtfs_snapshot_id(processing_date)

    selected_gtfs_snapshot = selected_gtfs_snapshot_id(AUDIT_PROCESSING_DATE)

    dbt_test_weekly_audits = BashOperator(
        task_id="dbt_test_weekly_audits",
        bash_command=dbt_command("test", "tag:audit", AUDIT_DBT_VARS, "--indirect-selection eager"),
    )

    selected_gtfs_snapshot >> dbt_test_weekly_audits


if __name__ == "__main__":
    dag.test()
