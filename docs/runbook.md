# Pipeline Runbook

## From-Scratch Rebuild

Raw BigQuery tables are rebuildable from immutable GCS inputs:

- GPS: `gs://ztm-analytics-bucket/raw/gps/**/part-*.parquet`
- GTFS: `gs://ztm-analytics-bucket/raw/gtfs/*.zip`

Rebuild order:

1. Recreate or empty the target v2 datasets: `ztm_raw`, `ztm_stg`, `ztm_int`, `ztm_marts`.
2. Rebuild `raw_gtfs_snapshots` deterministically from GCS object names and file hashes. Object names encode `snapshot_timestamp`; `snapshot_id` is `{snapshot_timestamp}_{sha256[:12]}`.
3. Reload each GTFS ZIP into `ztm_raw.raw_gtfs_*` with deterministic load job IDs.
4. Run GTFS staging, then rebuild archive-safe dimensions: `dim_line`, `dim_stop_post`, `dim_stop_group`, `dim_date`, and `dim_schedule_date`.
5. Rebuild schedule-version models from loaded GTFS history: `int_gtfs_trip_schedule`, `int_schedule_version`, and `dim_schedule_version`.
6. Build `_current` lookup tables only for the selected serving snapshot.
7. Reload GPS Parquet files into `ztm_raw.raw_gps_pings` with deterministic per-URI load job IDs.
8. Build dbt models by processing date and governing GTFS snapshot.
9. Validate backfilled facts before retiring `ztm_bq`.

Do not drop `ztm_bq` before the v2 datasets are validated.

## Per-Date Rebuild

For one GPS date, load raw GPS parts first, then run dbt with the Warsaw-local processing date and governing GTFS snapshot:

```bash
dbt build --select stg_gps__pings int_gps_hourly_completeness \
  --vars '{"processing_date":"YYYY-MM-DD"}'

dbt build --select stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__calendar_dates int_ping_trip \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'

dbt build --select stg_gtfs__stop_times stg_gtfs__stops int_stop_arrivals \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'

dbt build --select int_trip_summary \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'

dbt build --select fct_trip fct_stop_arrival \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"PROCESSING_DATE"}'

dbt build --select fct_trip fct_stop_arrival \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"PRIOR_SERVICE_DATE","aggregation_start_date":"PRIOR_SERVICE_DATE"}'
```

GPS staging and intermediate models use static-partition `insert_overwrite` for the selected `processing_date`. Serving facts are partitioned by `service_date` and overwrite `publish_service_date`. To complete overnight trips safely, the production DAG publishes both the current service date and the prior service date for each GPS processing date.

## Date-Range Backfill

Loop over dates in order. For every GPS processing date, ensure at least one loaded GTFS snapshot exists before that date. Schedule matching resolves governing snapshots per GTFS `service_date`: latest snapshot whose Warsaw-local `snapshot_timestamp` date is before the service date.

For each GPS processing date, rebuild the GPS/intermediate models, then publish facts for the current service date and the prior service date. After detail exists, rebuild completeness, coverage, aggregate, and pipeline-status marts over the collected-history window.

After backfill, verify that facts carry the expected `gtfs_snapshot_id` for each `service_date` by running the governing-snapshot tests on `fct_trip` and `fct_stop_arrival`. Also verify `schedule_version_id` resolves to a version covering the row's GPS processing date. A successful run is not enough; matching every historical date against the newest snapshot is silent corruption. Stop-arrival facts carry both publishing `gps_date` and `source_gps_date`; use `source_gps_date` when debugging which raw GPS partition produced an individual stop detection.

## Airflow Asset Graph

- `dag_gtfs_poll` produces `gtfs_snapshot` only when the GTFS ZIP hash changes.
- `dag_gtfs_load` consumes `gtfs_snapshot` and loads/tests the exact emitted snapshot.
- `dag_gps_raw_load` produces partitioned `raw_gps_date` events keyed by Warsaw-local GPS date. This records an hourly raw-load attempt, not a complete-day guarantee; completeness/status marts determine health.
- `dag_daily_gps` consumes `raw_gps_date`, rebuilds that date's warehouse graph, and emits `gps_models_date` after marts/status succeed.

Manual recovery remains explicit: trigger `dag_gtfs_load` with `snapshot_id`, `gcs_path`, and `processing_date`, or trigger `dag_daily_gps` with `processing_date` / partition key for the failed date. GTFS manual config must use `snapshot_id=YYYY-MM-DDTHH:MM:SSZ_<12 hex>`, `gcs_path=gs://ztm-analytics-bucket/raw/gtfs/{snapshot_id}.zip`, and `processing_date=YYYY-MM-DD`. Rerun failed date partitions rather than clearing unrelated dates.

## Manual Serving Export

`dag_serving_export` is manual-only for the alpha serving path. Run it after the mart tables are built and validated for the archive window you want to expose. It exports the fixed frontend source-table allowlist to GCS Parquet under `gs://ztm-analytics-bucket/serving/duckdb/staging/export_id=.../`, builds page-shaped DuckDB serving tables locally, validates the artifact, and atomically swaps the stable serving file. When changing the frontend serving surface, update the DAG source allowlist, derived-table SQL, tests, and `docs/serving_contract.md` together.

Default output path inside the Airflow container:

```text
/opt/airflow/serving/ztm.duckdb
```

Mount that directory to a stable VPS host path before using the export for the frontend. The frontend container should mount the same host path read-only and reopen DuckDB connections when `export_metadata.export_id` or the metadata JSON changes. A daily rebuild does not require a frontend container restart.

Useful manual config:

```json
{
  "export_id": "alpha-20260702",
  "output_dir": "/opt/airflow/serving",
  "output_filename": "ztm.duckdb",
  "max_source_bytes": 21474836480,
  "max_duckdb_bytes": 21474836480,
  "cleanup_gcs_staging": false
}
```

The Airflow image must include the `duckdb` Python package. The export fails before publication if required mart tables are missing, required serving tables are empty, source bytes exceed the configured guardrail, the built DuckDB file exceeds its guardrail, or validation cannot query the expected tables.

DuckDB builds run with the current VPS resource profile: `memory_limit='1GB'`, `max_temp_directory_size='2GB'`, `threads=2`, and `preserve_insertion_order=false`. The DuckDB memory limit is separate from `max_duckdb_bytes`, which only guards the final output file size. Resource-pressure failures leave the stable serving file unchanged; free disk/memory or reduce the export scope, then rerun with a fresh `export_id`.

Use a fresh `export_id` for every rerun. The export ID is embedded in deterministic BigQuery extract job IDs; failed or successful attempts reserve those job IDs even if GCS staging files are later removed.

The export queries BigQuery table metadata/date ranges, extracts tables to GCS, lists and downloads GCS staging objects, and writes the local serving file. If `cleanup_gcs_staging=true`, it also deletes staging objects after a successful export. Failed exports leave GCS staging files behind for inspection; remove them manually with:

```bash
gcloud storage rm --recursive gs://ztm-analytics-bucket/serving/duckdb/staging/export_id=EXPORT_ID/
```

Killed exports can also leave local hidden build artifacts under the serving directory. After confirming no serving export is running, remove them with:

```bash
rm -rf /opt/airflow/serving/.duckdb-tmp-EXPORT_ID \
  /opt/airflow/serving/.ztm.duckdb.EXPORT_ID.tmp*
```

The stable DuckDB file and sidecar JSON are not swapped transactionally as one unit. The DuckDB file is the source of truth for consumers; use `export_metadata` inside the database when exact consistency matters. The sidecar is `ztm.duckdb.meta.json` by default and mirrors the same export summary for operational inspection.

## Operational Notes

- Raw load retries are safe because job IDs are deterministic.
- One malformed GPS coordinate should be filtered at staging and must not fail stop-arrival reconstruction.
- Large ad-hoc BigQuery work should be dry-run and bounded by `maximum_bytes_billed`.
- Keep old `ztm_bq` objects until v2 validation passes.
