"""Planner DAG helpers: BigQuery extracts to Parquet via GCS, model bundles in GCS, the ztm-planner CLI.

All cloud I/O lives here; the planner component (``planner/``, run through PLANNER_COMMAND) only sees local
files. Extracts run as query + free extract job, so result rows never pass through Airflow's memory.
"""

from __future__ import annotations

import json
import logging
import os
import shlex
import shutil
import subprocess
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any

from google.cloud import bigquery, storage
from ztm_airflow_common import BIGQUERY_LOCATION

if TYPE_CHECKING:
    from datetime import date

LOGGER = logging.getLogger(__name__)
MODELS_PREFIX = "planner/models"
CURRENT_POINTER = f"{MODELS_PREFIX}/current.json"
EXTRACT_PREFIX = "planner/extracts"
FOOTPATHS_BLOB = "planner/footpaths/footpaths.parquet"
LIVE_CALIBRATION_BLOB = "planner/live/calibration.json"
# Geofabrik's regional extract (~300 MB, refreshed daily); it covers every ZTM stop.
OSM_PBF_URL = "https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf"
WARSAW_LAT, WARSAW_LON = 52.23, 21.01
WEATHER_FIELDS = "precipitation,snowfall,temperature_2m,wind_speed_10m"
DEFAULT_PLANNER_COMMAND = "/opt/airflow/planner-venv/bin/ztm-planner"
# Weekly stop tables bill ~5 GB each on 10 weeks; training segments ~6 GB; recent conditions ~0.5 GB.
MAX_BYTES_BILLED = 20 * 1024**3


@dataclass(frozen=True)
class PlannerConfig:
    """Runtime settings from the environment."""

    command: list[str]
    workdir: Path
    timeout_seconds: int

    @classmethod
    def from_env(cls) -> PlannerConfig:
        """Read PLANNER_COMMAND, PLANNER_WORKSPACE_ROOT and PLANNER_TIMEOUT_SECONDS."""
        return cls(
            command=shlex.split(os.getenv("PLANNER_COMMAND", DEFAULT_PLANNER_COMMAND)),
            workdir=Path(os.getenv("PLANNER_WORKSPACE_ROOT", "/opt/airflow/planner-work")),
            timeout_seconds=int(os.getenv("PLANNER_TIMEOUT_SECONDS", str(4 * 3600))),
        )


def export_parquet(
    client: bigquery.Client,
    bucket: storage.Bucket,
    sql: str,
    params: dict[str, date],
    gcs_prefix: str,
    local_dir: Path,
) -> list[Path]:
    """Run ``sql``, extract its result to GCS Parquet shards, download and delete them; returns local paths."""
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(k, "DATE", v) for k, v in params.items()],
            maximum_bytes_billed=MAX_BYTES_BILLED,
        ),
        location=BIGQUERY_LOCATION,
    )
    job.result()
    LOGGER.info("Extract %s billed %s bytes", gcs_prefix, job.total_bytes_billed)
    for blob in bucket.list_blobs(prefix=f"{gcs_prefix}/"):
        blob.delete()
    extract = client.extract_table(
        job.destination,
        f"gs://{bucket.name}/{gcs_prefix}/part-*.parquet",
        job_config=bigquery.ExtractJobConfig(destination_format="PARQUET", compression="SNAPPY"),
        location=BIGQUERY_LOCATION,
    )
    extract.result()
    local_dir.mkdir(parents=True, exist_ok=True)
    paths = []
    for blob in bucket.list_blobs(prefix=f"{gcs_prefix}/"):
        path = local_dir / Path(blob.name).name
        blob.download_to_filename(str(path))
        paths.append(path)
    if not paths:
        raise RuntimeError(f"Extract {gcs_prefix} produced no files")
    for blob in bucket.list_blobs(prefix=f"{gcs_prefix}/"):
        blob.delete()  # GCS only stages the extract; the local copy is what the planner reads
    return paths


def fetch_weather(url: str, path: Path) -> Path:
    """Download an Open-Meteo hourly response."""
    with urllib.request.urlopen(url, timeout=60) as response:  # noqa: S310 - fixed https Open-Meteo URLs
        path.write_bytes(response.read())
    hourly = json.loads(path.read_text(encoding="utf-8")).get("hourly", {})
    if not hourly.get("time"):
        raise RuntimeError(f"Weather response without hourly data: {url}")
    return path


def download(url: str, path: Path) -> Path:
    """Stream a large download to ``path``, replacing it only once complete."""
    tmp = path.with_name(path.name + ".partial")
    with urllib.request.urlopen(url, timeout=120) as response, tmp.open("wb") as out:  # noqa: S310 - fixed https URL
        shutil.copyfileobj(response, out, length=1 << 20)
    tmp.replace(path)
    return path


def query_rows(client: bigquery.Client, sql: str, params: dict[str, date]) -> list[dict[str, Any]]:
    """Run a query whose small result is wanted as rows."""
    job = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter(k, "DATE", v) for k, v in params.items()],
            maximum_bytes_billed=MAX_BYTES_BILLED,
        ),
        location=BIGQUERY_LOCATION,
    )
    rows = [dict(row) for row in job.result()]
    LOGGER.info("Query billed %s bytes for %d rows", job.total_bytes_billed, len(rows))
    return rows


def upload_live_calibration(bucket: storage.Bucket, calibration: dict[str, Any]) -> None:
    """Replace the live calibration scoring copies into the artifact."""
    bucket.blob(LIVE_CALIBRATION_BLOB).upload_from_string(json.dumps(calibration), content_type="application/json")


def download_live_calibration(bucket: storage.Bucket, path: Path) -> Path | None:
    """The latest live calibration, or None before the first weekly run (the artifact then has no live tables)."""
    blob = bucket.blob(LIVE_CALIBRATION_BLOB)
    if not blob.exists():
        LOGGER.warning("No live calibration at %s yet; the planner will not use live positions", LIVE_CALIBRATION_BLOB)
        return None
    blob.download_to_filename(str(path))
    return path


def upload_footpaths(bucket: storage.Bucket, path: Path) -> None:
    """Replace the footpaths scoring reads."""
    bucket.blob(FOOTPATHS_BLOB).upload_from_filename(str(path))


def download_footpaths(bucket: storage.Bucket, path: Path) -> Path | None:
    """The latest footpaths, or None before the first weekly build (scoring then estimates every walk)."""
    blob = bucket.blob(FOOTPATHS_BLOB)
    if not blob.exists():
        LOGGER.warning("No footpaths at %s yet; walks will be straight-line estimates", FOOTPATHS_BLOB)
        return None
    blob.download_to_filename(str(path))
    return path


def weather_archive_url(start: date, end: date) -> str:
    """Observed weather for training."""
    return (
        f"https://archive-api.open-meteo.com/v1/archive?latitude={WARSAW_LAT}&longitude={WARSAW_LON}"
        f"&start_date={start}&end_date={end}&hourly={WEATHER_FIELDS}&timezone=Europe%2FWarsaw"
    )


def weather_forecast_url(days: int) -> str:
    """Forecast for scoring; two past days feed the rolling rain and snow features."""
    return (
        f"https://api.open-meteo.com/v1/forecast?latitude={WARSAW_LAT}&longitude={WARSAW_LON}"
        f"&hourly={WEATHER_FIELDS}&past_days=2&forecast_days={days + 2}&timezone=Europe%2FWarsaw"
    )


def promotable(meta: dict[str, Any]) -> tuple[bool, str]:
    """A new bundle must beat the timetable and the lookup on its own held-out days."""
    mae = meta.get("metrics", {}).get("segment_mae_s", {})
    model, lookup, timetable = mae.get("model"), mae.get("lookup"), mae.get("timetable")
    if None in (model, lookup, timetable):
        return False, f"missing held-out metrics: {mae}"
    if not model < lookup < timetable:
        return False, f"held-out segment error must improve: timetable {timetable}, lookup {lookup}, model {model}"
    return True, f"held-out segment error: timetable {timetable}, lookup {lookup}, model {model}"


def upload_bundle(bucket: storage.Bucket, local_dir: Path, version: str) -> None:
    """Upload a bundle, then point ``current.json`` at it (the pointer moves only after every file is in)."""
    for path in sorted(local_dir.iterdir()):
        bucket.blob(f"{MODELS_PREFIX}/{version}/{path.name}").upload_from_filename(str(path))
    bucket.blob(CURRENT_POINTER).upload_from_string(json.dumps({"version": version}), content_type="application/json")


def download_current_bundle(bucket: storage.Bucket, cache_dir: Path) -> Path:
    """The current bundle, downloaded once per version."""
    pointer = bucket.blob(CURRENT_POINTER)
    if not pointer.exists():
        raise RuntimeError("No planner model has been promoted yet; run dag_planner_train first")
    version = json.loads(pointer.download_as_text())["version"]
    target = cache_dir / version
    if (target / "meta.json").exists():
        return target
    tmp = cache_dir / f".{version}.partial"
    tmp.mkdir(parents=True, exist_ok=True)
    for blob in bucket.list_blobs(prefix=f"{MODELS_PREFIX}/{version}/"):
        blob.download_to_filename(str(tmp / Path(blob.name).name))
    if not (tmp / "meta.json").exists():
        raise RuntimeError(f"Planner bundle {version} is incomplete")
    tmp.rename(target)
    for old in cache_dir.iterdir():  # one bundle a week otherwise piles up
        if old != target:
            shutil.rmtree(old, ignore_errors=True)
    return target


def run_planner(config: PlannerConfig, args: list[str]) -> dict[str, Any]:
    """Run ``ztm-planner`` and return its JSON summary (last stdout line)."""
    argv = [*config.command, *args]
    LOGGER.info("Running %s", " ".join(argv))
    # stderr is inherited, so progress and any traceback stream into the task log even when the command fails.
    result = subprocess.run(  # noqa: S603 - argv from configuration and fixed flags, no shell
        argv, check=True, timeout=config.timeout_seconds, stdout=subprocess.PIPE, text=True, shell=False
    )
    return json.loads(result.stdout.strip().splitlines()[-1])
