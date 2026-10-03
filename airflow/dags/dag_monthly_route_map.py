"""Monthly route delay map: BigQuery extract, corridor geometry, and publication beside the serving export."""

from __future__ import annotations

import logging
import os
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any
from zoneinfo import ZoneInfo

from airflow.sdk import DAG, CronTriggerTimetable, get_current_context, task
from google.cloud import bigquery
from route_map_build import check_mapped_share as check_route_map_mapped_share
from route_map_build import (
    expected_service_dates,
    month_dates,
    parse_extract,
    previous_month,
    publish,
    site_files,
    validate_extract,
)
from route_map_geometry import process
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    HISTORICAL_DAILY_ELIGIBLE_START_DATE,
    HISTORICAL_DAILY_EXCLUSION_REASONS,
    SERVING_EXPORT_DIR,
    airflow_failure_alert,
)

if TYPE_CHECKING:
    from collections.abc import Iterator

LOGGER = logging.getLogger(__name__)
SQL_DIR = Path(__file__).with_name("route_map_sql")
EXPECTED_STOP_EVENT_TABLE = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.fct_expected_stop_event"
RAW_GTFS_SHAPES_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_shapes"
# September 2026 billed 4.3 GB and 3.8 GB; caps leave room for busier months, not for runaway scans.
STATISTICS_MAX_BYTES_BILLED = 10 * 1024**3
POOLED_MAX_BYTES_BILLED = 10 * 1024**3
SNAPSHOT_IDS_MAX_BYTES_BILLED = 2 * 1024**3
# The nightly run for D republishes D-1, completing D-1's overnight trips.
# A month is final once the 1st of the next month has been published.
ROUTE_MAP_CRON = "0 12 2 * *"
WARSAW = ZoneInfo("Europe/Warsaw")


def _maps_dir() -> Path:
    return Path(os.getenv("SERVING_EXPORT_DIR", SERVING_EXPORT_DIR)) / "maps"


def _selected_month() -> str:
    context = get_current_context()
    dag_run = context.get("dag_run")
    conf = getattr(dag_run, "conf", None) or {}
    if conf.get("month"):
        month = str(conf["month"])
        month_dates(month)
        return month
    # Manual Airflow 3 runs may have no logical date; they then build the month before today.
    run_date = context.get("logical_date") or datetime.now(UTC)
    return previous_month(run_date.astimezone(WARSAW).date())


def _expected_dates(month: str) -> list[date]:
    return expected_service_dates(month, HISTORICAL_DAILY_ELIGIBLE_START_DATE, HISTORICAL_DAILY_EXCLUSION_REASONS)


def _check_month_published(client: bigquery.Client, month: str) -> None:
    """Every expected service date, and the next month's first day, must have published facts."""
    required = [*_expected_dates(month), month_dates(month)[-1] + timedelta(days=1)]
    query = "\n".join(
        (
            "select partition_id",
            f"from `{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}.INFORMATION_SCHEMA.PARTITIONS`",
            "where table_name = 'fct_expected_stop_event' and total_rows > 0",
            "  and partition_id in unnest(@partition_ids)",
        )
    )
    partition_ids = [day.strftime("%Y%m%d") for day in required]
    job_config = bigquery.QueryJobConfig(
        query_parameters=[bigquery.ArrayQueryParameter("partition_ids", "STRING", partition_ids)]
    )
    published = {row.partition_id for row in client.query(query, job_config=job_config).result()}
    missing = [day.isoformat() for day, partition_id in zip(required, partition_ids, strict=True) if partition_id not in published]
    if missing:
        raise RuntimeError(f"Route map for {month} needs published fct_expected_stop_event dates: {missing}")


def _sql(name: str, **tables: str) -> str:
    sql = (SQL_DIR / name).read_text(encoding="utf-8")
    for token, table in tables.items():
        sql = sql.replace(f"{{{{ {token} }}}}", table)
    if "{{" in sql:
        raise ValueError(f"Unresolved table token in {name}")
    return sql


def _extract_rows(client: bigquery.Client, month: str) -> Iterator[tuple[str, str]]:
    """Run both extract queries and stream (kind, payload) rows; never holds the result as one string."""
    days = month_dates(month)
    statistics_job = client.query(
        _sql("segment_statistics.sql", fct_expected_stop_event=EXPECTED_STOP_EVENT_TABLE),
        job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=STATISTICS_MAX_BYTES_BILLED,
            query_parameters=[
                bigquery.ScalarQueryParameter("month_start", "DATE", days[0]),
                bigquery.ScalarQueryParameter("month_end", "DATE", days[-1]),
            ],
        ),
    )
    statistics_job.result()
    # The anonymous result table lives for 24 hours, long enough for the pooling query.
    statistics_table = f"{statistics_job.destination.project}.{statistics_job.destination.dataset_id}.{statistics_job.destination.table_id}"
    LOGGER.info("Segment statistics for %s billed %s bytes", month, statistics_job.total_bytes_billed)

    snapshot_job = client.query(
        "\n".join(
            (
                "select distinct json_value(payload, '$.gtfs_snapshot_id') as gtfs_snapshot_id",
                f"from `{statistics_table}`",
                "where kind = 'segment'",
            )
        ),
        job_config=bigquery.QueryJobConfig(maximum_bytes_billed=SNAPSHOT_IDS_MAX_BYTES_BILLED),
    )
    snapshot_ids = sorted(row.gtfs_snapshot_id for row in snapshot_job.result())

    pooled_job = client.query(
        _sql("pooled_routes.sql", segment_statistics=statistics_table, raw_gtfs_shapes=RAW_GTFS_SHAPES_TABLE),
        job_config=bigquery.QueryJobConfig(
            maximum_bytes_billed=POOLED_MAX_BYTES_BILLED,
            query_parameters=[bigquery.ArrayQueryParameter("snapshot_ids", "STRING", snapshot_ids)],
        ),
    )
    rows = pooled_job.result()
    LOGGER.info(
        "Pooled routes for %s (%d snapshots) billed %s bytes, %s rows",
        month,
        len(snapshot_ids),
        pooled_job.total_bytes_billed,
        rows.total_rows,
    )
    for row in rows:
        yield row.kind, row.payload


def _build_route_map(month: str) -> dict[str, Any]:
    client = bigquery.Client(project=GCP_PROJECT)
    _check_month_published(client, month)
    data = parse_extract(_extract_rows(client, month))
    validate_extract(data, _expected_dates(month))
    geojson, report = process(data, month)
    del data
    check_route_map_mapped_share(report)
    target = publish(_maps_dir(), month, site_files(geojson, report, month))
    summary = {
        "month": month,
        "path": str(target),
        "corridors": len(geojson["features"]),
        "daytime": {
            period: {key: values[key] for key in ("input_traversals", "mapped_traversals")}
            for period, values in report["time_windows"]["daytime"]["periods"].items()
        },
    }
    LOGGER.info("Published route map: %s", summary)
    return summary


with DAG(
    dag_id="dag_monthly_route_map",
    dag_display_name="Monthly route delay map",
    description="Build last month's route delay map from published facts and publish it beside the serving export.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronTriggerTimetable(ROUTE_MAP_CRON, timezone=WARSAW),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "serving", "map"],
) as dag:

    @task
    def build_route_map() -> dict[str, Any]:
        """Extract, map, and publish one month; trigger with {"month": "YYYY-MM"} to rebuild or backfill."""
        return _build_route_map(_selected_month())

    build_route_map()


if __name__ == "__main__":
    dag.test()
