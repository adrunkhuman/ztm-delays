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
| `BIGQUERY_MATCHER_INPUT_DATASET` | `ztm_matcher_input` |
| `BIGQUERY_LOCATION` | `europe-north1` |
| `GCS_BUCKET` | `ztm-analytics-bucket` |
| `DBT_PROJECT_DIR` | `/opt/airflow/dbt` |
| `RAW_GPS_PREFIX` | `raw/gps` |
| `RAW_GTFS_PREFIX` | `raw/gtfs` |
| `MATCHER_ENABLED` | `false` |
| `BIGQUERY_MATCHER_STAGING_DATASET` | Required when enabled; no default |
| `MATCHER_WORKSPACE_ROOT` | `/opt/airflow/matcher-work` |
| `MATCHER_COMMAND` | `uv run --locked --project /opt/airflow/matcher ztm-matcher` |
| `MATCHER_PROJECT_DIR` | `/opt/airflow/matcher` |
| `MATCHER_TIMEOUT_SECONDS` | `2700` |
| `MATCHER_GCS_PREFIX` | `matcher/runs` |
| `MATCHER_KEEP_WORKSPACE` | `false` |
| `MATCHER_MAX_GPS_OBJECTS` | `5000` |
| `MATCHER_MAX_GPS_BYTES` | `21474836480` (20 GiB) |
| `MATCHER_MIN_FREE_DISK_BYTES` | `5368709120` (5 GiB) |
| `MATCHER_MAX_MARKER_BYTES` | `20971520` (20 MiB) |
| `MATCHER_MAX_RSS_BYTES` | `2147483648` (2 GiB) |
| `MATCHER_MAX_PUBLICATION_BYTES` | `5368709120` (5 GiB) |

- Airflow and dbt use the same `GCP_PROJECT` / `BIGQUERY_*` env names.
- `dbt/` is mounted at `DBT_PROJECT_DIR`.
- `/opt/airflow/serving` is writable by Airflow when serving exports are enabled.
- The frontend reads the same serving host directory as DuckDB plus `.meta.json`; it does not read BigQuery or GCS.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- The Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, `google-cloud-storage`, `duckdb`, `numpy`, `pyarrow`, `pytz`, `uv`, and Python 3.13.
- The service account can read/write the configured GCS bucket and load/query the configured BigQuery datasets.
- Mount the matcher source read-only at `/opt/airflow/matcher`. Set `UV_PROJECT_ENVIRONMENT` to a writable path outside that mount, and keep `MATCHER_WORKSPACE_ROOT` writable.

## DAG Boundaries

| DAG ID | UI name | Trigger | Owns | Does not own |
| --- | --- | --- | --- | --- |
| `dag_gtfs_poll` | GTFS snapshot poll | Hourly cron | Download GTFS ZIP, hash it, store changed snapshots, emit `gtfs_snapshot`. | GTFS raw loading or dbt models. |
| `dag_gtfs_load` | GTFS snapshot load | `gtfs_snapshot` asset | Load GTFS raw tables, run GTFS staging, rebuild dimensions and schedule-version models. | GPS processing or broad manual schedule audits. |
| `dag_gps_raw_load` | GPS raw ingest | Hourly cron | Load available poller Parquet parts into raw BigQuery, emit `raw_gps_date`. | Completeness judgment or warehouse modeling. |
| `dag_daily_gps` | GPS nightly warehouse | Nightly cron | Rebuild one GPS processing date, publish current/prior facts, run bounded marts/status, emit `gps_models_date`. | Hourly raw ingestion or full-history audit tests. |
| `dag_serving_export` | Serving DuckDB export | `gps_models_date` asset or manual recovery | Export the fixed frontend source allowlist, build DuckDB, write `.meta.json`, atomically publish the serving artifact. | Warehouse rebuilds or live poller streaming. |

## Normal Runs

- `dag_gtfs_poll` and `dag_gps_raw_load` are frequent ingestion DAGs.
- `dag_gtfs_load` runs only when a changed GTFS snapshot is emitted.
- `dag_daily_gps` runs once per night, uses the persisted governing snapshot for its processing date, republishes the current and prior service dates, and accepts a manual `processing_date` for targeted recovery.
- `dag_serving_export` runs from the partitioned GPS-model asset; manual recovery runs use a fresh `export_id`.
- Default dbt tests stay bounded. Full-history schedule/version and broad aggregate audits are manual jobs.

## Python Matcher

`dag_daily_gps` runs the Python matcher as its only reconstruction path. It starts after snapshot selection and GPS staging validation, downloads immutable GPS/GTFS inputs, invokes the bounded matcher, validates the outputs, and loads content-addressed run tables.

Production requires `MATCHER_ENABLED=true`, an isolated `BIGQUERY_MATCHER_STAGING_DATASET`, the matcher source mount, and writable workspace and `uv` environment paths.

Before publication, Airflow verifies artifact schemas, hashes, snapshot lineage, row grains, processing dates, non-empty outputs, bus/tram coverage, accepted-execution counts, peak RSS, and zero swap. It then replaces the four stable `ztm_matcher_input` partitions in one BigQuery transaction. The stable dataset and tables are created idempotently on first publication.

The three fact artifacts are partitioned by `gps_date`; stop semantics is partitioned by `processing_date`. dbt publishes `fct_trip`, `fct_stop_arrival`, and `fct_expected_stop_event` for current and prior service dates, then rebuilds coverage, status, and serving marts.

Minimal recovery is to fix the matcher/configuration and rerun the processing date. Raw GPS and GTFS remain immutable, stable input replacement is atomic, fact publication uses partition overwrite, and serving export keeps its previous artifact until a new export succeeds.

`matcher_historical_correction.py plan` produces bounded, read-only plans for explicitly approved historical corrections.
The retained service range starts on `2026-06-27`; raw processing dates `2026-06-26` and `2026-07-05` through `2026-07-07` are excluded. The valid boundary runs on `2026-06-27` and `2026-07-08` set `skip_prior_publication=true`, so they reconstruct current-date data without replacing the excluded prior partition.
Plans require a validated `--plan-id`; use a new ID for a retry. They emit plan-ID-scoped deterministic Airflow 3 run IDs, the expected GTFS snapshot in each trigger configuration, and a bounded `wait-for-dag-run` command after every trigger.

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
