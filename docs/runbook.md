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
9. Validate backfilled facts, completeness, coverage, and serving export inputs before exposing rebuilt data.

The old `ztm_bq` dataset has been removed; recovery now targets `ztm_raw`, `ztm_stg`, `ztm_int`, and `ztm_marts`.

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

## Raw GPS Volume

Measure raw GPS object volume from GCS metadata before changing poller flush cadence or adding a local durable spool:

```bash
cd poller
uv run python measure_raw_gps_volume.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

The command defaults to `gs://ztm-analytics-bucket/raw/gps`; pass `--bucket` and `--prefix` for other environments. It does not query BigQuery. Use `--include-row-counts` only for a small bounded window when row counts are needed, because it downloads each matched Parquet object to read file metadata. Production durability uses periodic append-safe GCS flushes plus a bounded local JSON spool at `/var/lib/ztm-poller-spool`; compressed GCS volume is only a lower-bound sizing proxy. Revisit the default 100 MiB cap only with measured object/byte volume, expected outage duration, flush cadence, explicit disk budget, and crash-loss tolerance.

## Recovery And Alerting

Airflow orchestration DAGs use bounded transient retries by default: two retries with a five-minute delay. Failure watcher tasks keep `retries=0` so persistent upstream failures still make the DAG run fail loudly after retries are exhausted. Raw BigQuery load retries are safe because load job IDs are deterministic; dbt retries are only for transient execution failures and do not hide data-quality failures after the retry budget is spent. Manual serving export does not retry because its deterministic extract jobs are tied to one `export_id`; rerun it with a new `export_id` after fixing the failure.

Set `AIRFLOW_FAILURE_WEBHOOK_URL` to an HTTPS endpoint to receive structured task/DAG failure callbacks. If it is unset, failures are still logged in Airflow. Do not clear failed historical DAG runs just to make the UI green; leave persistent data-quality failures as incident evidence until the underlying issue is understood. Clear or rerun only after fixing transient infrastructure issues, correcting configuration, or intentionally reprocessing a partition/snapshot.

The serving export reads the private poller heartbeat from `POLLER_HEARTBEAT_GCS_PATH` or `health/poller/latest.json`, sanitizes it, and writes `poller_status` plus `last_export_at` into `ztm.duckdb.meta.json`. Heartbeats older than 180 seconds are exported as `stale`. Missing or malformed heartbeat data is exported as `unknown`, not as a serving-export failure.

## Deployment Sync

Deploy steps:

- join the Tailnet as `tag:github-actions`;
- SSH to `ubuntu@vps`;
- fail on tracked VPS worktree changes;
- run `git -C /home/ubuntu/ztm-pipeline pull --ff-only origin master`;
- smoke-check Airflow DAG parsing, `airflow dags list`, and `dbt parse` inside the Airflow container.

It does not rebuild containers or run dbt models.

Required GitHub secrets:

- `TS_OAUTH_CLIENT_ID`
- `TS_OAUTH_SECRET`
- `VPS_DEPLOY_SSH_KEY`
- `VPS_DEPLOY_KNOWN_HOSTS`

Optional GitHub vars override defaults: `VPS_DEPLOY_HOST`, `VPS_DEPLOY_USER`, `VPS_REPO_DIR`, and `AIRFLOW_CONTAINER_PREFIX`.

Keep SSH Tailscale-only. The workflow reaches the VPS through a tagged ephemeral Tailscale node.

Emergency hotfixes must be committed and pushed, or reverted intentionally, before automated deploys can resume.

## Airflow Cadence And Asset Graph

DAG boundaries follow schedule, retry, and recovery semantics. TaskGroups may improve a DAG's graph view, but they do not replace separate DAGs with different triggers or recovery paths. The serving export is a publication step after marts exist, not part of ingestion/modeling.

- `dag_gtfs_poll` produces `gtfs_snapshot` only when the GTFS ZIP hash changes.
- `dag_gtfs_load` consumes `gtfs_snapshot` and loads/tests the exact emitted snapshot.
- `dag_gps_raw_load` produces partitioned `raw_gps_date` events keyed by Warsaw-local GPS date. This records an hourly raw-load attempt, not a complete-day guarantee; completeness/status marts determine health.
- `dag_daily_gps` runs nightly at `04:00 Europe/Warsaw`, rebuilds one GPS processing date, and emits `gps_models_date` after marts/status succeed. The DAG ID is historical; hourly raw GPS asset events no longer trigger full warehouse rebuilds.

Manual recovery remains explicit: trigger `dag_gtfs_load` with `snapshot_id`, `gcs_path`, and `processing_date`, or trigger `dag_daily_gps` with `processing_date` / partition key for the failed date. GTFS manual config must use `snapshot_id=YYYY-MM-DDTHH:MM:SSZ_<12 hex>`, `gcs_path=gs://ztm-analytics-bucket/raw/gtfs/{snapshot_id}.zip`, and `processing_date=YYYY-MM-DD`. Rerun failed date partitions rather than clearing unrelated dates.

## dbt Test Tiers

Normal Airflow cadence must stay bounded and deliberate:

- Hourly raw GPS loading only loads immutable GCS parts into raw BigQuery.
- Nightly GPS warehouse work runs one processing date and its prior service-date fact publication.
- `mart_day_completeness`, `agg_service_coverage`, `agg_line_daily`, and `mart_pipeline_status` replace the prior/current date partitions during normal nightly runs.
- Period aggregate marts replace affected month-start and schedule-version-start `period_start_date` partitions; Airflow computes the required source start date from affected month starts and active schedule-version starts.
- Nightly aggregate marts build but their broad tests are manual audit jobs.
- GTFS load runs raw load, staging, dimensions, and cheap/default dimension tests.

Expensive tests are manual audit jobs until operational maturity is higher. Do not add them back to default Airflow DAG paths.

Nightly Airflow still tests `mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status`; only the four broad serving aggregate tests moved to manual audits.

Manual GTFS schedule audit:

```bash
dbt test --select int_gtfs_trip_schedule int_schedule_version \
  --indirect-selection cautious --exclude test_type:unit \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

The schedule audit intentionally uses compact singular contract tests for required fields and accepted values, plus uniqueness/range/relationship tests. Do not re-add repeated generic column tests to these expensive views without a fresh byte estimate.

Manual aggregate/fact audit for an aggregate build window. Run this before aggregate contract changes, serving-impacting changes, or periodic manual audits; do not put this selector back in the normal nightly path without a fresh byte estimate. The vars should match the aggregate mart build window. For a smaller audit window, rebuild the aggregates in a dev or dedicated audit dataset first.

```bash
dbt test --select fct_trip fct_stop_arrival mart_day_completeness agg_service_coverage agg_line_daily agg_line_stop_period agg_stop_period agg_time_period mart_pipeline_status \
  --indirect-selection cautious --exclude test_type:unit \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"YYYY-MM-DD","aggregation_start_date":"YYYY-MM-DD","period_source_start_date":"YYYY-MM-DD","period_partition_dates":"YYYY-MM-DD|YYYY-MM-DD"}'
```

For `mart_day_completeness`, `agg_service_coverage`, `agg_line_daily`, and `mart_pipeline_status`, normal recovery should rerun each affected processing date so the prior/current partition pair is replaced. Wider manual backfills can pass a wider `aggregation_start_date`, but dry-run first because every date in that inclusive range becomes an overwrite partition.

For period aggregates, `aggregation_start_date` is the affected processing-date window, while `period_source_start_date` is the earliest affected month start or active schedule-version `valid_from_date` needed to recompute those affected rows. `period_partition_dates` is a pipe-delimited list of target `period_start_date` partitions. Omit `period_partition_dates` only for explicit dynamic/full-window rebuilds after a dry-run.

After `dag_daily_gps` finishes its normal dbt phases, it logs a BigQuery dbt cost summary from `INFORMATION_SCHEMA.JOBS_BY_USER`: job count, total bytes processed, total bytes billed, and top jobs by bytes. This is visibility only. Metadata collection failure is logged but does not block asset publication. Attribution is best-effort: it is scoped to the same BigQuery principal, project, and region, and filters on dbt query comments, so concurrent dbt jobs from the same principal can be included while jobs from another principal or without dbt comments can be missed.

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
- Default Airflow dbt tests must stay cheap enough for normal cadence; full-history schedule/version tests are manual audit work.
- The old `ztm_bq` dataset is gone; rebuild and recovery work should target the v2 datasets.
