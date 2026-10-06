"""Manual, one-off: seed feed-health baselines from the raw GPS archive.

Hourly poller summaries only exist since collection began, so same-weekday
baselines would otherwise take three weeks to warm up. This writes one seed per
UTC hour under ``health/poller/baseline-seed/`` for the lookback before
collection. Each holds the fresh fleet per minute: distinct vehicles with a ping
in the trailing five minutes, the poller's MAX_PING_AGE_SECONDS rule. This
matches the poller's per-poll fresh counts within about 2%. Seeds are only read
where no summary exists. Rerunning overwrites them idempotently.

Conf: {"days": 28} (1..28). The query scans three columns of ``raw_gps_pings``,
about 0.2 GB per day.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import Any

from airflow.sdk import DAG, get_current_context, task
from google.cloud import bigquery, storage
from poller_health import MAXIMUM_LOOKBACK_DAYS, SEED_MAX_BYTES, build_seeds, hour_path, timestamp
from poller_health_gcs import collection_start, first_collection_start
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_LOCATION,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    airflow_failure_alert,
)

SEED_QUERY = f"""
with presence as (
  select distinct vehicle_type, VehicleNumber, timestamp_trunc(Time, minute) as t
  from `{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gps_pings`
  where Time >= timestamp_sub(@start, interval 5 minute) and Time < @end and vehicle_type in (1, 2)
)
select if(vehicle_type = 1, 'bus', 'tram') as mode,
       timestamp_add(t, interval k minute) as minute,
       count(distinct VehicleNumber) as fleet
from presence, unnest(generate_array(0, 4)) as k
where timestamp_add(t, interval k minute) >= @start and timestamp_add(t, interval k minute) < @end
group by 1, 2
"""  # noqa: S608 - identifiers come from deployment configuration; times are parameters


def seed_window(collection: datetime, days: int) -> tuple[datetime, datetime]:
    """Whole UTC hours strictly before the first collected hour."""
    if not 1 <= days <= MAXIMUM_LOOKBACK_DAYS:
        raise ValueError("days must be 1..28")
    end = collection.astimezone(UTC).replace(minute=0, second=0, microsecond=0)
    return end - timedelta(days=days), end


def write_seeds(bucket: storage.Bucket, seeds: dict[datetime, dict[str, Any]]) -> int:
    """Upload bounded seeds under their UTC hour; returns the number written."""
    for hour, seed in seeds.items():
        payload = json.dumps(seed, sort_keys=True, separators=(",", ":"))
        if len(payload.encode()) > SEED_MAX_BYTES:
            raise ValueError("seed exceeds bound")
        bucket.blob(hour_path("baseline-seed", hour)).upload_from_string(payload, content_type="application/json")
    return len(seeds)


def run_seed(bucket: storage.Bucket, client: bigquery.Client, days: int) -> int:
    """Seed the ``days`` before collection began; collection must already have started."""
    marker = collection_start(bucket) or first_collection_start(bucket)
    if marker is None:
        raise ValueError("collection start unknown; deploy the collector before seeding")
    start, end = seed_window(timestamp(marker), days)
    job = client.query(
        SEED_QUERY,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start", "TIMESTAMP", start),
                bigquery.ScalarQueryParameter("end", "TIMESTAMP", end),
            ]
        ),
        location=BIGQUERY_LOCATION,
    )
    rows = [(row["mode"], row["minute"], row["fleet"]) for row in job.result()]
    return write_seeds(bucket, build_seeds(rows))


with DAG(
    dag_id="poller_health_seed",
    dag_display_name="Poller feed health: seed baselines",
    schedule=None,
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["poller", "health"],
) as dag:

    @task
    def seed_baselines() -> int:
        """Read conf days (default 28) and write seeds."""
        conf = getattr(get_current_context().get("dag_run"), "conf", None) or {}
        days = conf.get("days", MAXIMUM_LOOKBACK_DAYS)
        if type(days) is not int:
            raise ValueError("conf days must be an integer")
        return run_seed(
            storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET), bigquery.Client(project=GCP_PROJECT), days
        )

    seed_baselines()
