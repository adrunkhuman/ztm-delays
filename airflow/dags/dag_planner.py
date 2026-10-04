"""Trip planner: weekly model training and the nightly planner artifact beside the serving export.

dag_planner_train (Sunday 13:00): ten weeks of observed segments and stop arrivals -> model bundle in GCS,
promoted only if it beats the timetable and the lookup on its own held-out week; then, whatever the model gate
decided, walking distances between nearby stop posts from the OSM extract -> footpaths in GCS.
dag_planner_score (06:30, after the 04:00 warehouse run adds yesterday): current timetable + promoted bundle +
recent conditions + weather forecast -> <serving>/planner/planner.duckdb for the frontend's planner tab.
"""

from __future__ import annotations

import logging
import os
import shutil
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import planner_queries as queries
from airflow.sdk import DAG, CronTriggerTimetable, TriggerRule, task
from google.cloud import bigquery, storage
from planner_pipeline import (
    OSM_PBF_URL,
    PlannerConfig,
    download,
    download_current_bundle,
    download_footpaths,
    export_parquet,
    fetch_weather,
    promotable,
    run_planner,
    upload_bundle,
    upload_footpaths,
    weather_archive_url,
    weather_forecast_url,
)
from ztm_airflow_common import (
    AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    BIGQUERY_MARTS_DATASET,
    BIGQUERY_RAW_DATASET,
    GCP_PROJECT,
    GCS_BUCKET,
    SERVING_EXPORT_DIR,
    airflow_failure_alert,
)

LOGGER = logging.getLogger(__name__)
WARSAW = ZoneInfo("Europe/Warsaw")
TRAIN_WEEKS = 10
CALIBRATION_DAYS = 7  # the planner's stage A holds out the window's last week
RECENT_DAYS = 8
HORIZON_DAYS = 7
MARTS = f"{GCP_PROJECT}.{BIGQUERY_MARTS_DATASET}"
RAW_GTFS_SNAPSHOTS_TABLE = f"{GCP_PROJECT}.{BIGQUERY_RAW_DATASET}.raw_gtfs_snapshots"


def _today() -> date:
    return datetime.now(WARSAW).date()


def _latest_published_date(client: bigquery.Client) -> date:
    row = next(iter(client.query(
        f"select max(service_date) as d from `{MARTS}.fct_trip` where service_date >= date_sub(current_date(), interval 14 day)"
    ).result()))  # fmt: skip
    if row.d is None:
        raise RuntimeError("No published trips in the last 14 days")
    return row.d


def _train(version: str) -> dict[str, Any]:
    config = PlannerConfig.from_env()
    client, bucket = bigquery.Client(project=GCP_PROJECT), storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    end = _latest_published_date(client)
    start = end - timedelta(weeks=TRAIN_WEEKS) + timedelta(days=1)
    work = config.workdir / "train" / version
    shutil.rmtree(work, ignore_errors=True)
    inputs = work / "inputs"
    window = {"start": start, "end": end}
    export_parquet(
        client,
        bucket,
        queries.training_segments(MARTS),
        window,
        f"planner/extracts/{version}/segments",
        inputs / "segments",
    )
    slots = export_parquet(
        client,
        bucket,
        queries.stop_slots(MARTS),
        window,
        f"planner/extracts/{version}/stop_slots",
        inputs / "stop_slots",
    )
    eps = export_parquet(
        client, bucket, queries.stop_eps(MARTS), {**window, "cal_start": end - timedelta(days=CALIBRATION_DAYS - 1)},
        f"planner/extracts/{version}/stop_eps", inputs / "stop_eps",
    )  # fmt: skip
    weather = fetch_weather(weather_archive_url(start, end), inputs / "weather.json")
    if len(slots) != 1 or len(eps) != 1:
        _merge_parquet(slots, inputs / "stop_slots.parquet")
        _merge_parquet(eps, inputs / "stop_eps.parquet")
    meta = run_planner(config, [
        "--workdir", str(work / "run"), "train",
        "--segments", str(inputs / "segments" / "*.parquet"), "--weather-json", str(weather),
        "--stop-slots", str(_single(slots, inputs / "stop_slots.parquet")),
        "--stop-eps", str(_single(eps, inputs / "stop_eps.parquet")),
        "--start", start.isoformat(), "--end", end.isoformat(), "--version", version,
        "--bundle-out", str(work / "bundle"),
    ])  # fmt: skip
    ok, reason = promotable(meta)
    if not ok:
        raise RuntimeError(f"Planner model {version} not promoted: {reason}")
    upload_bundle(bucket, work / "bundle", version)
    LOGGER.info("Promoted planner model %s: %s", version, reason)
    shutil.rmtree(inputs, ignore_errors=True)
    return meta


def _footpaths() -> dict[str, Any]:
    config = PlannerConfig.from_env()
    client, bucket = bigquery.Client(project=GCP_PROJECT), storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    work = config.workdir / "footpaths"
    shutil.rmtree(work, ignore_errors=True)
    work.mkdir(parents=True)
    snapshot, _ = _snapshots(client, _today())
    gtfs_zip = work / "gtfs.zip"
    storage.Blob.from_string(snapshot.gcs_path, client=bucket.client).download_to_filename(str(gtfs_zip))
    pbf = download(OSM_PBF_URL, work / "region.osm.pbf")
    output = work / "footpaths.parquet"
    summary = run_planner(config, [
        "--workdir", str(work / "run"), "footpaths", "--osm-pbf", str(pbf), "--gtfs-zip", str(gtfs_zip),
        "--output", str(output),
    ])  # fmt: skip
    upload_footpaths(bucket, output)
    LOGGER.info("Published footpaths for GTFS %s: %s", snapshot.snapshot_id, summary)
    shutil.rmtree(work, ignore_errors=True)
    return {**summary, "gtfs_snapshot_id": snapshot.snapshot_id}


def _snapshots(client: bigquery.Client, today: date) -> tuple[Any, Any]:
    """Latest GTFS snapshot, and the last one taken before today (it still lists yesterday's night trips)."""
    rows = list(client.query(
        f"""
        (select 'latest' as kind, snapshot_id, gcs_path from `{RAW_GTFS_SNAPSHOTS_TABLE}`
         order by snapshot_timestamp desc limit 1)
        union all
        (select 'previous' as kind, snapshot_id, gcs_path from `{RAW_GTFS_SNAPSHOTS_TABLE}`
         where snapshot_timestamp < timestamp(@today, 'Europe/Warsaw') order by snapshot_timestamp desc limit 1)
        """,
        job_config=bigquery.QueryJobConfig(query_parameters=[bigquery.ScalarQueryParameter("today", "DATE", today)]),
    ).result())  # fmt: skip
    by_kind = {row.kind: row for row in rows}
    if "latest" not in by_kind:
        raise RuntimeError("No GTFS snapshot found")
    return by_kind["latest"], by_kind.get("previous")


def _single(paths: list[Path], merged: Path) -> Path:
    return paths[0] if len(paths) == 1 else merged


def _merge_parquet(paths: list[Path], target: Path) -> None:
    """Small stop tables may still be sharded by the extract job; the planner expects one file."""
    import duckdb  # noqa: PLC0415 - in the Airflow image already, only needed here

    files = ", ".join(f"'{p}'" for p in paths)
    with duckdb.connect() as con:
        con.execute(f"copy (select * from read_parquet([{files}])) to '{target}' (format parquet)")


def _score(run_id: str) -> dict[str, Any]:
    config = PlannerConfig.from_env()
    client, bucket = bigquery.Client(project=GCP_PROJECT), storage.Client(project=GCP_PROJECT).bucket(GCS_BUCKET)
    work = config.workdir / "score"
    shutil.rmtree(work / "inputs", ignore_errors=True)
    inputs = work / "inputs"
    bundle = download_current_bundle(bucket, config.workdir / "bundles")
    today = _today()
    snapshot, previous = _snapshots(client, today)
    inputs.mkdir(parents=True, exist_ok=True)
    gtfs_zip, previous_zip = inputs / "gtfs.zip", inputs / "gtfs_previous.zip"
    storage.Blob.from_string(snapshot.gcs_path, client=bucket.client).download_to_filename(str(gtfs_zip))
    previous_args: list[str] = []
    if previous is not None and previous.snapshot_id != snapshot.snapshot_id:
        storage.Blob.from_string(previous.gcs_path, client=bucket.client).download_to_filename(str(previous_zip))
        previous_args = ["--previous-gtfs-zip", str(previous_zip)]
    recent = export_parquet(
        client, bucket, queries.recent_daily(MARTS),
        {"start": today - timedelta(days=RECENT_DAYS), "end": today - timedelta(days=1)},
        f"planner/extracts/score-{run_id}/recent", inputs / "recent",
    )  # fmt: skip
    if len(recent) != 1:
        _merge_parquet(recent, inputs / "recent.parquet")
    weather = fetch_weather(weather_forecast_url(HORIZON_DAYS), inputs / "forecast.json")
    footpaths = download_footpaths(bucket, inputs / "footpaths.parquet")
    footpath_args = ["--footpaths", str(footpaths)] if footpaths else []
    output = Path(os.getenv("SERVING_EXPORT_DIR", SERVING_EXPORT_DIR)) / "planner" / "planner.duckdb"
    summary = run_planner(config, [
        "--workdir", str(work / "run"), "score", "--bundle", str(bundle), "--gtfs-zip", str(gtfs_zip), *previous_args,
        "--recent-daily", str(_single(recent, inputs / "recent.parquet")), "--weather-json", str(weather),
        "--start", today.isoformat(), "--days", str(HORIZON_DAYS), "--output", str(output), *footpath_args,
    ])  # fmt: skip
    LOGGER.info("Published planner artifact from GTFS %s: %s", snapshot.snapshot_id, summary)
    shutil.rmtree(inputs, ignore_errors=True)
    return {**summary, "gtfs_snapshot_id": snapshot.snapshot_id}


with DAG(
    dag_id="dag_planner_train",
    dag_display_name="Planner model training",
    description="Weekly travel-time model training; promotes a bundle that beats the timetable and lookup.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronTriggerTimetable("0 13 * * 0", timezone="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "planner"],
) as train_dag:

    @task(retries=0)
    def train_model() -> dict[str, Any]:
        """Extract, train and promote; a failed gate keeps the previous model."""
        return _train(datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"))

    # After training rather than beside it: both need ~1.5-3 GB and the VPS has 4 GB for them.
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def build_footpaths() -> dict[str, Any]:
        """OSM walking distances between nearby posts; scoring keeps the previous ones if this fails."""
        return _footpaths()

    @task(trigger_rule=TriggerRule.ALL_SUCCESS)
    def training_complete() -> None:
        """Keep a training failure visible even when the all-done footpath task succeeds."""

    trained, walked, complete = train_model(), build_footpaths(), training_complete()
    trained >> walked
    trained >> complete
    walked >> complete


with DAG(
    dag_id="dag_planner_score",
    dag_display_name="Planner nightly artifact",
    description="Predict the coming week's timetable and publish planner.duckdb beside the serving export.",
    start_date=datetime(2026, 1, 1, tzinfo=UTC),
    schedule=CronTriggerTimetable("30 6 * * *", timezone="Europe/Warsaw"),
    catchup=False,
    max_active_runs=1,
    default_args=AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS,
    on_failure_callback=airflow_failure_alert,
    tags=["ztm", "planner", "serving"],
) as score_dag:

    @task
    def publish_planner(run_id: str | None = None) -> dict[str, Any]:
        """Score and publish; the frontend keeps the previous artifact if this fails."""
        # Airflow injects run_id; it only names this run's temporary GCS extracts.
        return _score((run_id or datetime.now(UTC).isoformat()).replace(":", "").replace("+", ""))

    publish_planner()
