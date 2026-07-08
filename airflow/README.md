# Airflow DAGs

DAG files in this directory are mounted into the Airflow container. DAG IDs are stable operational IDs; use `dag_display_name` for UI wording instead of renaming IDs and splitting history.

## Runtime Contract

Runtime env defaults match the current VPS:

| Env var | Default |
| --- | --- |
| `GCP_PROJECT` | `ztm-data` |
| `BIGQUERY_RAW_DATASET` | `ztm_raw` |
| `BIGQUERY_STG_DATASET` | `ztm_stg` |
| `BIGQUERY_INT_DATASET` | `ztm_int` |
| `BIGQUERY_MARTS_DATASET` | `ztm_marts` |
| `BIGQUERY_LOCATION` | `europe-north1` |
| `GCS_BUCKET` | `ztm-analytics-bucket` |
| `DBT_PROJECT_DIR` | `/opt/airflow/dbt` |
| `RAW_GPS_PREFIX` | `raw/gps` |
| `RAW_GTFS_PREFIX` | `raw/gtfs` |

- Airflow and dbt use the same `GCP_PROJECT` / `BIGQUERY_*` env names.
- `dbt/` is mounted at `DBT_PROJECT_DIR`.
- `/opt/airflow/serving` is writable by Airflow when serving exports are enabled.
- The frontend reads the same serving host directory as DuckDB plus `.meta.json`; it does not read BigQuery or GCS.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- The Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, `google-cloud-storage`, and `duckdb`.
- The service account can read/write the configured GCS bucket and load/query the configured BigQuery datasets.

## DAG Boundaries

| DAG ID | UI name | Trigger | Owns | Does not own |
| --- | --- | --- | --- | --- |
| `dag_gtfs_poll` | GTFS snapshot poll | Hourly cron | Download GTFS ZIP, hash it, store changed snapshots, emit `gtfs_snapshot`. | GTFS raw loading or dbt models. |
| `dag_gtfs_load` | GTFS snapshot load | `gtfs_snapshot` asset | Load GTFS raw tables, run GTFS staging, rebuild dimensions and schedule-version models. | GPS processing or broad manual schedule audits. |
| `dag_gps_raw_load` | GPS raw ingest | Hourly cron | Load available poller Parquet parts into raw BigQuery, emit `raw_gps_date`. | Completeness judgment or warehouse modeling. |
| `dag_daily_gps` | GPS nightly warehouse | Nightly cron | Rebuild one GPS processing date, publish current/prior facts, run bounded marts/status, emit `gps_models_date`. | Hourly raw ingestion or full-history audit tests. |
| `dag_serving_export` | Serving DuckDB export | Manual | Export the fixed frontend source allowlist, build DuckDB, write `.meta.json`, atomically publish the serving artifact. | Warehouse rebuilds or live poller streaming. |

## Normal Runs

- `dag_gtfs_poll` and `dag_gps_raw_load` are frequent ingestion DAGs.
- `dag_gtfs_load` runs only when a changed GTFS snapshot is emitted.
- `dag_daily_gps` runs once per night, uses the latest dimension-built GTFS snapshot available at rebuild time, republishes the current and prior service dates, and accepts a manual `processing_date` for targeted recovery.
- `dag_serving_export` is manual; use a fresh `export_id` for every run.
- Default dbt tests stay bounded. Full-history schedule/version and broad aggregate audits are manual jobs.

## Serving Export

Default settings:

```text
SERVING_EXPORT_DIR=/opt/airflow/serving
SERVING_EXPORT_FILENAME=ztm.duckdb
SERVING_EXPORT_GCS_PREFIX=serving/duckdb/staging
SERVING_EXPORT_MAX_BYTES=21474836480
SERVING_EXPORT_MAX_SOURCE_BYTES=21474836480
SERVING_EXPORT_MAX_DUCKDB_BYTES=21474836480
```

`SERVING_EXPORT_MAX_SOURCE_BYTES` and `SERVING_EXPORT_MAX_DUCKDB_BYTES` default to `SERVING_EXPORT_MAX_BYTES`.

Manual `dag_run.conf` may override `export_id`, `output_dir`, `output_filename`, `gcs_bucket`, `gcs_prefix`, `max_source_bytes`, `max_duckdb_bytes`, and `cleanup_gcs_staging`.

Use one shared host directory for Airflow and frontend serving mounts. Airflow needs write access; frontend should only need read access. On the current VPS the bind-mounted host directory should be writable by the Airflow container user:

```bash
sudo install -d -o 50000 -g 0 -m 0775 /home/ubuntu/ztm-pipeline/serving
```

Failed exports leave GCS staging files for inspection. Remove them manually when no longer needed:

```bash
gcloud storage rm --recursive gs://ztm-analytics-bucket/serving/duckdb/staging/export_id=EXPORT_ID/
```

## Manual Recovery

Trigger `dag_gtfs_load` manually only with explicit snapshot context:

```json
{
  "snapshot_id": "YYYY-MM-DDTHH:MM:SSZ_<12 hex>",
  "gcs_path": "gs://ztm-analytics-bucket/raw/gtfs/{snapshot_id}.zip",
  "processing_date": "YYYY-MM-DD"
}
```

Trigger `dag_daily_gps` manually with one `processing_date`. Do not clear or backfill broad date ranges without a fresh byte estimate.

## Local Tests

Local Windows/minimal-Python environments need `tzdata` for `ZoneInfo("Europe/Warsaw")`:

```bash
uv run --with pytest --with duckdb --with tzdata pytest airflow/tests
```
