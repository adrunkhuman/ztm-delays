from __future__ import annotations

import json

from airflow.sdk import Asset

GCP_PROJECT = "ztm-data"
BIGQUERY_RAW_DATASET = "ztm_raw"
BIGQUERY_MARTS_DATASET = "ztm_marts"
BIGQUERY_LOCATION = "europe-north1"
GCS_BUCKET = "ztm-analytics-bucket"
DBT_PROJECT_DIR = "/opt/airflow/dbt"
RAW_GTFS_PREFIX = "raw/gtfs"

GTFS_SNAPSHOT_ASSET = Asset("x-ztm://gtfs/snapshot")
RAW_GPS_DATE_ASSET = Asset("x-ztm://gps/raw-date")
GPS_MODELS_DATE_ASSET = Asset("x-ztm://gps/models-date")


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
    return f"cd {DBT_PROJECT_DIR} && dbt {subcommand} --select {selector}{args} --vars '{dbt_vars}'"
