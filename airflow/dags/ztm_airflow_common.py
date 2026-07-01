from __future__ import annotations

try:
    from airflow.sdk import Asset
except ImportError:  # Airflow 2 compatibility for local parser checks and older images.
    from airflow import Dataset as Asset

GCP_PROJECT = "ztm-data"
BIGQUERY_RAW_DATASET = "ztm_raw"
BIGQUERY_LOCATION = "europe-north1"
GCS_BUCKET = "ztm-analytics-bucket"
DBT_PROJECT_DIR = "/opt/airflow/dbt"

GTFS_SNAPSHOT_ASSET = Asset("x-ztm://gtfs/snapshot")
RAW_GPS_DATE_ASSET = Asset("x-ztm://gps/raw-date")
GPS_MODELS_DATE_ASSET = Asset("x-ztm://gps/models-date")


def dbt_command(subcommand: str, selector: str, dbt_vars: str, extra_args: str = "") -> str:
    """Build a dbt CLI command with consistent project directory and vars quoting."""
    args = f" {extra_args}" if extra_args else ""
    return f"cd {DBT_PROJECT_DIR} && dbt {subcommand} --select {selector}{args} --vars '{dbt_vars}'"
