from __future__ import annotations

import json
import logging
import os
import shlex
from datetime import timedelta
from typing import TYPE_CHECKING, Any
from urllib.parse import urlsplit
from urllib.request import Request, urlopen

from airflow.sdk import Asset

if TYPE_CHECKING:
    from collections.abc import Mapping

LOGGER = logging.getLogger(__name__)


def _env(name: str, default: str) -> str:
    return os.getenv(name, default).strip() or default


GCP_PROJECT = _env("GCP_PROJECT", "ztm-data")
BIGQUERY_RAW_DATASET = _env("BIGQUERY_RAW_DATASET", "ztm_raw")
BIGQUERY_STG_DATASET = _env("BIGQUERY_STG_DATASET", "ztm_stg")
BIGQUERY_INT_DATASET = _env("BIGQUERY_INT_DATASET", "ztm_int")
BIGQUERY_MARTS_DATASET = _env("BIGQUERY_MARTS_DATASET", "ztm_marts")
BIGQUERY_LOCATION = _env("BIGQUERY_LOCATION", "europe-north1")
GCS_BUCKET = _env("GCS_BUCKET", "ztm-analytics-bucket")
DBT_PROJECT_DIR = _env("DBT_PROJECT_DIR", "/opt/airflow/dbt")
RAW_GPS_PREFIX = _env("RAW_GPS_PREFIX", "raw/gps")
RAW_GTFS_PREFIX = _env("RAW_GTFS_PREFIX", "raw/gtfs")
AIRFLOW_TRANSIENT_RETRIES = 2
AIRFLOW_TRANSIENT_RETRY_DELAY = timedelta(minutes=5)
AIRFLOW_FAILURE_WEBHOOK_TIMEOUT_SECONDS = 10.0

SERVING_EXPORT_DIR = _env("SERVING_EXPORT_DIR", "/opt/airflow/serving")
SERVING_EXPORT_GCS_PREFIX = _env("SERVING_EXPORT_GCS_PREFIX", "serving/duckdb/staging")
SERVING_EXPORT_FILENAME = _env("SERVING_EXPORT_FILENAME", "ztm.duckdb")
SERVING_EXPORT_MAX_BYTES = _env("SERVING_EXPORT_MAX_BYTES", str(20 * 1024 * 1024 * 1024))

GTFS_SNAPSHOT_ASSET = Asset("x-ztm://gtfs/snapshot")
RAW_GPS_DATE_ASSET = Asset("x-ztm://gps/raw-date")
GPS_MODELS_DATE_ASSET = Asset("x-ztm://gps/models-date")


def airflow_failure_alert(context: Mapping[str, Any]) -> None:
    """Emit a bounded failure alert without depending on Airflow metadata DB access."""
    payload = _airflow_failure_payload(context)
    LOGGER.error("Airflow task failed: %s", json.dumps(payload, sort_keys=True))

    webhook_url = os.getenv("AIRFLOW_FAILURE_WEBHOOK_URL", "").strip()
    if not webhook_url:
        return
    if urlsplit(webhook_url).scheme != "https":
        LOGGER.error("AIRFLOW_FAILURE_WEBHOOK_URL must be HTTPS")
        return

    request = Request(  # noqa: S310
        webhook_url,
        data=json.dumps(payload, sort_keys=True).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=AIRFLOW_FAILURE_WEBHOOK_TIMEOUT_SECONDS):  # noqa: S310
            return
    except Exception:
        LOGGER.exception("Failed to send Airflow failure webhook")


def _airflow_failure_payload(context: Mapping[str, Any]) -> dict[str, object]:
    task_instance = context.get("task_instance") or context.get("ti")
    dag_run = context.get("dag_run")
    return {
        "dag_id": _context_attr(task_instance, "dag_id") or _context_attr(dag_run, "dag_id"),
        "task_id": _context_attr(task_instance, "task_id"),
        "run_id": _context_attr(task_instance, "run_id") or _context_attr(dag_run, "run_id"),
        "try_number": _context_attr(task_instance, "try_number"),
        "map_index": _context_attr(task_instance, "map_index"),
        "logical_date": _context_iso(context.get("logical_date")),
        "exception_type": type(context.get("exception")).__name__ if context.get("exception") is not None else None,
    }


def _context_attr(value: object, name: str) -> object:
    return getattr(value, name, None)


def _context_iso(value: object) -> str | None:
    isoformat = getattr(value, "isoformat", None)
    return isoformat() if callable(isoformat) else None


AIRFLOW_TRANSIENT_RETRY_DEFAULT_ARGS = {
    "retries": AIRFLOW_TRANSIENT_RETRIES,
    "retry_delay": AIRFLOW_TRANSIENT_RETRY_DELAY,
    "on_failure_callback": airflow_failure_alert,
}


def gtfs_snapshot_id(snapshot_timestamp: str, file_hash: str) -> str:
    """Build the immutable GTFS snapshot identifier used across GCS and BigQuery."""
    return f"{snapshot_timestamp}_{file_hash[:12]}"


def gtfs_gcs_path(snapshot_id: str) -> str:
    """Build the canonical GCS object path for a GTFS snapshot ZIP."""
    return f"{RAW_GTFS_PREFIX}/{snapshot_id}.zip"


def gtfs_gcs_uri(snapshot_id: str) -> str:
    """Build the canonical GCS URI for a GTFS snapshot ZIP."""
    return f"gs://{GCS_BUCKET}/{gtfs_gcs_path(snapshot_id)}"


def dbt_vars(**values: str) -> str:
    """Serialize dbt vars without hand-built JSON strings."""
    return json.dumps(values)


def dbt_command(subcommand: str, selector: str, dbt_vars: str, extra_args: str = "") -> str:
    """Build a dbt CLI command with consistent project directory and vars quoting."""
    args = f" {extra_args}" if extra_args else ""
    if subcommand == "test":
        args += " --exclude test_type:unit"
    return f"cd {shlex.quote(DBT_PROJECT_DIR)} && dbt {subcommand} --select {selector}{args} --vars '{dbt_vars}'"
