# Pipeline Runbook

## From-Scratch Rebuild

Raw BigQuery tables are rebuildable from immutable GCS inputs:

- GPS: `gs://ztm-analytics-bucket/raw/gps/**/part-*.parquet`
- GTFS: `gs://ztm-analytics-bucket/raw/gtfs/*.zip`

Rebuild order:

1. Recreate or empty the target datasets: `ztm_raw`, `ztm_stg`, `ztm_int`, `ztm_marts`, and `ztm_matcher_input`.
1. Rebuild `raw_gtfs_snapshots` deterministically from GCS object names and file hashes. Object names encode
   `snapshot_timestamp`; `snapshot_id` is `{snapshot_timestamp}_{sha256[:12]}`.
1. Reload each GTFS ZIP into `ztm_raw.raw_gtfs_*` with deterministic load job IDs.
1. Run GTFS staging, then rebuild archive-safe dimensions: `dim_line`, `dim_stop_post`, `dim_stop_group`, `dim_date`,
   and `dim_schedule_date`.
1. Rebuild schedule-version models from loaded GTFS history: `int_gtfs_trip_schedule`, `int_schedule_version`, and
   `dim_schedule_version`.
1. Build `_current` lookup tables only for the selected serving snapshot.
1. Reload GPS Parquet files into `ztm_raw.raw_gps_pings` with deterministic per-URI load job IDs.
1. Build dbt models by processing date and selected GTFS snapshot.
1. Validate backfilled facts, completeness, coverage, and serving export inputs before exposing rebuilt data.

The old `ztm_bq` dataset has been removed; recovery now targets `ztm_raw`, `ztm_stg`, `ztm_int`, `ztm_marts`, and `ztm_matcher_input`.

## Per-Date Rebuild

For one GPS date, load raw GPS parts, run the matcher with the mapped snapshot, promote its validated artifacts, then publish dbt facts for the current and prior service dates. The scheduled DAG owns this sequence; prefer a targeted DAG run over manual commands.

```bash
dbt build --select stg_gps__pings int_gps_hourly_completeness \
  --vars '{"processing_date":"YYYY-MM-DD"}'

dbt build --select fct_trip fct_stop_arrival fct_expected_stop_event \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"PROCESSING_DATE"}'

dbt build --select fct_trip fct_stop_arrival fct_expected_stop_event \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"PRIOR_SERVICE_DATE","aggregation_start_date":"PRIOR_SERVICE_DATE"}'
```

Matcher inputs are partitioned by processing date. Serving facts are partitioned by `service_date` and overwrite `publish_service_date`. To complete overnight trips safely, the
production DAG publishes both the current service date and the prior service date for each GPS processing date. A prior
service-date partition can contain rows from two GPS dates with different governing GTFS snapshots; fact publication
preserves each row's snapshot lineage and joins schedule metadata on `gtfs_snapshot_id`.

When an older processing date `D` republishes current service date `D`, the fact models replace rows produced by `D` but
retain rows already published from `gps_date > D`. Those newer rows are after-midnight overlays from a later matcher run;
dropping them would regress the service-date partition to its pre-overnight state. For the same trip, the newer overlay wins.

`insert_overwrite` replaces the listed partitions even when the compiled source query returns zero rows. Historical
reruns must derive the governing GTFS snapshot from the warehouse processing-date mapping, not from the latest snapshot,
and must stop on schedule-version or snapshot-lineage test failures before continuing to later dates. Those expensive
lineage checks are tagged `audit`, excluded from normal DAG test paths, and run by `dag_weekly_audit`; if you run
recovery manually, run the audit selector explicitly rather than relying on default DAG tests.

## Date-Range Backfill

Generate an immutable plan with `matcher_historical_correction.py plan --plan-id ID --start-date YYYY-MM-DD
--end-date YYYY-MM-DD --refresh-through-date LATEST_PUBLISHED_DATE --report-json PLAN.json`, then review its exact snapshot, GCS inventory, affected partitions,
and final serving-refresh dates before execution. The retained eligible range starts on `2026-06-27`. Dates `2026-07-05` through
`2026-07-07` are excluded because of the confirmed GPS outage; `2026-06-26` is also excluded because collection began
mid-day. July 12 remains eligible because lower Sunday tram volume is expected service, not an outage.

Each run uses the persisted governing snapshot from `int_gtfs_processing_snapshot`. Do not substitute the newest loaded
snapshot. Execute the reviewed plan inside the Airflow container with `matcher_historical_correction.py execute
--plan-json PLAN.json`. The controller uses deterministic run IDs, skips successful dates on resume, stops on the first
non-successful existing run, and never queues a later date before the current date succeeds. Clear only the failed child
run after diagnosis, then execute the same plan again.

For each GPS processing date, correction mode reruns matcher publication, current/prior facts, completeness, coverage,
and pipeline status. It skips the archive-wide matcher schedule dependencies, serving marts, and per-date asset event.
Boundary plans explicitly use current-only input and omit the excluded prior partition. Normal prior publication
incorporates after-midnight observations without relabeling prior-day rows to the current processing date's snapshot.
Every dbt command uses the plan's `maximum_bytes_billed` guard.

After every correction date succeeds, the controller triggers `dag_historical_serving_refresh` once. That DAG rebuilds
each deduplicated serving date once, restores the final schedule view, rebuilds full serving dimensions once, and emits
one explicit `dag_serving_export` run for the consolidated DuckDB generation. If correction fails, serving and
export do not run. If only serving refresh fails, clear that refresh run and resume the same plan without rerunning facts.

After backfill, verify that facts carry the expected `gtfs_snapshot_id` for each processing batch and that
`schedule_version_id` resolves to a version covering the row's GPS processing date. Stop-arrival facts carry both
publishing `gps_date` and `source_gps_date`; use `source_gps_date` when debugging which raw GPS partition produced an
individual stop detection.

Changes to reconstruction do not update existing partitions on deployment. Reprocess each affected GPS date with its mapped snapshot, then republish current/prior facts and dependent marts. A raw reload is not required.

## Raw GPS Volume

Measure raw GPS object volume from GCS metadata before changing poller flush cadence or adding a local durable spool:

```bash
cd poller
uv run python measure_raw_gps_volume.py --start-date YYYY-MM-DD --end-date YYYY-MM-DD
```

The command defaults to `gs://ztm-analytics-bucket/raw/gps`; pass `--bucket` and `--prefix` for other environments. It
does not query BigQuery. Use `--include-row-counts` only for a small bounded window when row counts are needed, because
it downloads each matched Parquet object to read file metadata. Production durability uses periodic append-safe GCS
flushes plus a bounded local JSON spool at `/var/lib/ztm-poller-spool`; compressed GCS volume is only a lower-bound
sizing proxy. Revisit the default 100 MiB cap only with measured object/byte volume, expected outage duration, flush
cadence, explicit disk budget, and crash-loss tolerance.

## Recovery And Alerting

Airflow orchestration DAGs use bounded transient retries by default: two retries with a five-minute delay. Failure
watcher tasks keep `retries=0` so persistent upstream failures still make the DAG run fail loudly after retries are
exhausted. Raw BigQuery load retries are safe because load job IDs are deterministic; dbt retries are only for transient
execution failures and do not hide data-quality failures after the retry budget is spent. Manual serving export does not
retry because its deterministic extract jobs are tied to one `export_id`; rerun it with a new `export_id` after fixing
the failure.

Set `AIRFLOW_FAILURE_WEBHOOK_URL` to an HTTPS endpoint to receive structured task/DAG failure callbacks. If it is unset,
failures are still logged in Airflow. Do not clear failed historical DAG runs just to make the UI green; leave
persistent data-quality failures as incident evidence until the underlying issue is understood. Clear or rerun only
after fixing transient infrastructure issues, correcting configuration, or intentionally reprocessing a
partition/snapshot.

The serving export reads the private poller heartbeat from `POLLER_HEARTBEAT_GCS_PATH` or `health/poller/latest.json`,
sanitizes it, and writes `poller_status` plus `last_export_at` into `ztm.duckdb.meta.json`. Heartbeats older than 180
seconds are exported as `stale`. Missing or malformed heartbeat data is exported as `unknown`, not as a serving-export
failure.

## Matcher Runs

`dag_daily_gps` uses the Python matcher as its sole reconstruction path.

Matcher runs require `MATCHER_ENABLED=true`, isolated staging and matcher-input datasets, the read-only matcher source at
`/opt/airflow/matcher`, a writable `MATCHER_WORKSPACE_ROOT`, and a separate
`UV_PROJECT_ENVIRONMENT` so `uv` never writes to the matcher mount.

For each processing date, the DAG reads immutable GPS inputs and the pinned GTFS snapshot, then runs the bounded matcher.
Before publication, it verifies non-empty artifacts, exact schemas and snapshot lineage, unique grains, accepted-execution
counts, complete mode coverage, and peak RSS and current process swap no greater than their configured limits.

After validation, the DAG idempotently creates the stable matcher-input dataset and tables when absent, then atomically
replaces the three fact partitions at `gps_date = processing_date` plus the stop-semantics `processing_date` partition. It
publishes current and prior service-date dbt facts only after publication, followed by coverage, pipeline-status, and
serving marts. A validation failure leaves the previous stable partitions and canonical facts unchanged.

Retries and recovery rerun the same processing date from immutable GPS and pinned GTFS inputs. They must not select a
newer snapshot implicitly. Historical plans pass an inventory digest for the exact snapshot and GPS object metadata;
the matcher re-lists inputs and rejects additions or replacements before invocation. A legacy single-date validated
marker cannot prove this identity: use a new Airflow run ID for controlled recovery rather than retrying that run.
`matcher_historical_correction.py` creates read-only plans for separately approved retained-history work; it verifies
the mapped snapshot and GCS inventories and emits date-specific preflight and trigger commands, but it does not
download, load, publish, or mutate warehouse data itself.

Run-scoped matcher tables are diagnostic/retry artifacts, not a recovery boundary. `matcher_run_*` load tables and
`matcher_run_stage_*` publication tables expire after `MATCHER_STAGING_RETENTION_DAYS` (three days by default).
Publication stages are deleted best-effort only after stable-table post-validation and `published.json` creation; native
expiration remains the fallback for failed runs and cleanup failures. The four stable matcher-input tables do not use
either transient prefix and must never receive expiration.

GCS `pending.json` and `validated.json` markers are retained for three days by default; `published.json` is retained for
30 days. Any matcher load attempt performs best-effort marker cleanup before invoking the matcher, so failed attempts do
not require a later successful publication for eventual marker cleanup. Immutable raw GPS/GTFS objects plus a fresh,
deterministic run remain the recovery boundary after transient artifacts expire.

Metadata-only staging audit:

```sql
with transient_tables as (
    select
        table_catalog,
        table_schema,
        table_name,
        total_logical_bytes,
        creation_time
    from `ztm-data.region-europe-north1.INFORMATION_SCHEMA.TABLE_STORAGE`
    where (table_schema = 'ztm_matcher_stage' and starts_with(table_name, 'matcher_run_'))
       or (table_schema = 'ztm_matcher_input' and starts_with(table_name, 'matcher_run_stage_'))
),

expirations as (
    select table_catalog, table_schema, table_name, option_value as expiration_timestamp
    from `ztm-data.region-europe-north1.INFORMATION_SCHEMA.TABLE_OPTIONS`
    where option_name = 'expiration_timestamp'
)

select
    tables.table_schema,
    count(*) as table_count,
    sum(total_logical_bytes) as total_logical_bytes,
    min(creation_time) as oldest_created,
    max(creation_time) as newest_created,
    countif(expiration_timestamp is null) as no_expiration_count,
    min(expiration_timestamp) as earliest_expiration,
    max(expiration_timestamp) as latest_expiration
from transient_tables as tables
left join expirations using (table_catalog, table_schema, table_name)
group by tables.table_schema
order by tables.table_schema;
```

## Deployment Sync

Deploy steps:

- join the Tailnet as `tag:github-actions`;
- SSH to `ubuntu@vps`;
- fail on tracked VPS worktree changes;
- run `git -C /home/ubuntu/ztm-pipeline pull --ff-only origin master`;
- verify host/container matcher hashes, canonical matcher environment and mounts, Airflow DAG parsing, `airflow dags list`, and `dbt parse` inside the Airflow container.

It does not rebuild containers or run dbt models. If the matcher bind hash is stale, redeploy Airflow in Coolify and rerun the workflow.

Required GitHub secrets:

- `TS_OAUTH_CLIENT_ID`
- `TS_OAUTH_SECRET`
- `VPS_DEPLOY_SSH_KEY`
- `VPS_DEPLOY_KNOWN_HOSTS`

Optional GitHub vars override defaults: `VPS_DEPLOY_HOST`, `VPS_DEPLOY_USER`, `VPS_REPO_DIR`, and
`AIRFLOW_CONTAINER_PREFIX`.

Keep SSH Tailscale-only. The workflow reaches the VPS through a tagged ephemeral Tailscale node.

Emergency hotfixes must be committed and pushed, or reverted intentionally, before automated deploys can resume.

## Airflow Cadence And Asset Graph

DAG boundaries follow schedule, retry, and recovery semantics. TaskGroups may improve a DAG's graph view, but they do
not replace separate DAGs with different triggers or recovery paths. The serving export is a publication step after
marts exist, not part of ingestion/modeling.

- `dag_gtfs_poll` produces `gtfs_snapshot` only when the GTFS ZIP hash changes.
- `dag_gtfs_load` consumes `gtfs_snapshot` and loads/tests the exact emitted snapshot.
- `dag_gps_raw_load` produces partitioned `raw_gps_date` events keyed by Warsaw-local GPS date. This records an hourly
  raw-load attempt, not a complete-day guarantee; completeness/status marts determine health.
- `dag_daily_gps` runs nightly at `04:00 Europe/Warsaw`, rebuilds one GPS processing date plus prior-date facts/serving
  partitions, and emits `gps_models_date` with both changed partition dates after marts/status succeed. The DAG ID is
  historical; hourly raw GPS asset events no longer trigger full warehouse rebuilds.

Manual recovery remains explicit: trigger `dag_gtfs_load` with `snapshot_id`, `gcs_path`, and `processing_date`, or
trigger `dag_daily_gps` with `processing_date` / partition key for the failed date. GTFS manual config must use
`snapshot_id=YYYY-MM-DDTHH:MM:SSZ_<12 hex>`, `gcs_path=gs://ztm-analytics-bucket/raw/gtfs/{snapshot_id}.zip`, and
`processing_date=YYYY-MM-DD`. Rerun failed date partitions rather than clearing unrelated dates.

## dbt Test Tiers

Normal Airflow cadence must stay bounded and deliberate:

- Hourly raw GPS loading only loads immutable GCS parts into raw BigQuery.
- Nightly GPS warehouse work runs one processing date and its prior service-date fact publication.
- `mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status` replace current and prior partitions
  independently with each date's governing GTFS snapshot. The DAG restores current-snapshot schedule views before
  serving models run.
- Incremental serving marts rebuild the prior date before the current date after pipeline status succeeds; full-table
  serving models run only with the current date.
- GTFS load runs raw load, staging, dimensions, and cheap/default dimension tests.

Expensive tests are audit jobs until operational maturity is higher. Do not add them back to default Airflow DAG paths.

Audit-tagged tests are real tests, not vacuous pass-through SQL. Normal Airflow DAG tests exclude `tag:audit`;
`dag_weekly_audit` refreshes audit-tagged evidence models, then runs `dbt test --select tag:audit` weekly. Manual
recovery/backfill procedures that need lineage assurance must run the tagged models before the same test selector and
stop on failure before continuing to later dates.

Nightly Airflow tests `mart_day_completeness`, `agg_service_coverage`, `mart_pipeline_status`, and the serving marts.
Retired serving aggregate tests stay out of the default path.

Manual GTFS schedule audit:

```bash
dbt test --select tag:audit \
  --indirect-selection eager --exclude test_type:unit \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

The schedule audit intentionally uses compact singular contract tests for required fields and accepted values, plus
uniqueness/range/relationship tests. Do not re-add repeated generic column tests to these expensive views without a
fresh byte estimate.

Manual fact/status audit for a recovery window. Run this before fact/status contract changes, serving-impacting changes,
or periodic manual audits; do not put this selector back in the normal nightly path without a fresh byte estimate. The
vars should match the recovery window.

```bash
dbt test --select fct_trip fct_stop_arrival mart_day_completeness agg_service_coverage mart_pipeline_status \
  --indirect-selection cautious --exclude test_type:unit \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID","publish_service_date":"YYYY-MM-DD","aggregation_start_date":"YYYY-MM-DD"}'
```

For `mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status`, normal recovery should rerun each
affected date with `processing_date` and `aggregation_start_date` set to that same date and with that date's governing
snapshot. Do not rebuild a multi-date range under one snapshot. Wider manual backfills must switch the schedule views
and snapshot variable per date.

After `dag_daily_gps` finishes its normal dbt phases, it logs a BigQuery dbt cost summary from
`INFORMATION_SCHEMA.JOBS_BY_USER`: job count, total bytes processed, total bytes billed, and top jobs by bytes. This is
visibility only. Metadata collection failure is logged but does not block asset publication. Attribution is best-effort:
it is scoped to the same BigQuery principal, project, and region, and filters on dbt query comments, so concurrent dbt
jobs from the same principal can be included while jobs from another principal or without dbt comments can be missed.

## Serving Export

`dag_serving_export` normally consumes the `gps_models_date` asset after nightly marts succeed. It can also be triggered
manually after a wider mart rebuild. It exports the fixed frontend source-table allowlist to GCS Parquet under
`gs://ztm-analytics-bucket/serving/duckdb/staging/export_id=.../`, builds and validates the DuckDB artifact locally, and
atomically swaps the stable serving file. When changing the frontend serving surface, update the DAG source allowlist,
tests, and `docs/serving_contract.md` together.

Default output path inside the Airflow container:

```text
/opt/airflow/serving/ztm.duckdb
```

The equivalent environment overrides are `SERVING_EXPORT_VALIDATION_TIMEOUT_SECONDS`,
`SERVING_EXPORT_VALIDATION_MEMORY_LIMIT_MB`, `SERVING_EXPORT_VALIDATION_TEMP_LIMIT_MB`, and
`SERVING_EXPORT_VALIDATION_THREADS`. Values supplied in `dag_run.conf` take precedence over environment values.

Mount that directory to a stable VPS host path before using the export for the frontend. The frontend container should
mount the same host path read-only and reopen DuckDB connections when `export_metadata.export_id` or the metadata JSON
changes. A daily rebuild does not require a frontend container restart.

Useful manual config:

```json
{
  "export_id": "alpha-20260702",
  "output_dir": "/opt/airflow/serving",
  "output_filename": "ztm.duckdb",
  "changed_partition_dates": ["2026-07-01", "2026-07-02"],
  "max_source_bytes": 21474836480,
  "max_duckdb_bytes": 21474836480,
  "validation_timeout_seconds": 600,
  "validation_memory_limit_mb": 4096,
  "validation_temp_limit_mb": 6144,
  "validation_threads": 1,
  "cleanup_gcs_staging": false,
  "staging_retention_days": 3
}
```

Manual partition-cache exports must list every date changed by the preceding rebuild. Omit `changed_partition_dates` to
extract complete source tables when the changed set is unknown.

Deployments that introduce or change canonical serving models must rebuild `int_serving_trip_execution`,
`int_serving_stop_arrival`, and their dependent serving marts for every retained serving date before unpausing
`dag_serving_export`. Verify `mart_trip_daily.gtfs_snapshot_id` is non-null across the retained range before
publication. This is a one-time migration; normal nightly runs replace only prior/current serving partitions.

The Airflow image must include the `duckdb` Python package. The export fails before publication if required mart tables
are missing, required serving tables are empty, source bytes exceed the configured guardrail, the built DuckDB file
exceeds its guardrail, or structural or semantic validation fails.

DuckDB builds run with the current VPS resource profile: `memory_limit='1GB'`, `max_temp_directory_size='2GB'`,
`threads=2`, and `preserve_insertion_order=false`. The DuckDB memory limit is separate from `max_duckdb_bytes`, which
only guards the final output file size. Resource-pressure failures leave the stable serving file unchanged; free
disk/memory or reduce the export scope, then rerun with a fresh `export_id`.

Semantic validation defaults to a 4 GiB process/DuckDB limit, 6 GiB spill limit, one thread, and a 10-minute timeout.
These values were required by the successful 20-date historical export. The current Coolify profile uses no explicit
memory limit and host swappiness `60`; the application-level validator limit remains the primary guard against memory
pressure. Verify the effective container memory and swap settings after redeployment with `docker inspect`, because
editing Coolify's generated Compose file or applying `docker update` is not persistent.

Semantic validation runs in an isolated process against the temporary DuckDB before publication. It applies a hard
wall-clock timeout plus its own DuckDB memory, spill, and thread limits. Deterministic checks cover serving-date alignment,
pipeline-status identities and bounds, unique trip/event keys, and trip-to-stop-event relationships for changed dates plus
the latest serving date. Missing serving dates, incomplete GPS days, absent status modes, and zero scheduled-service
coverage are warnings rather than publication failures. The sidecar records the complete validation summary under
`semantic_validation`; embedded `export_metadata` records `semantic_validation_status` and
`semantic_validation_warnings_json`. Validation does not issue BigQuery query jobs.

Warning codes are `serving_date_absent`, `pipeline_status_mode_absent`, `incomplete_gps_day`, and
`zero_service_coverage`. Records include `service_date` and, where applicable, `mode`, `completeness_ratio`, or
`expected_trips`. At most 100 warning records are included; `warning_count` and `warnings_truncated` expose omissions.
The complete child report is capped at 64 KiB, and exceeding that cap blocks publication.

Use a fresh `export_id` for every rerun. The export ID is embedded in deterministic BigQuery extract job IDs; failed or
successful attempts reserve those job IDs even if GCS staging files are later removed.

The export queries BigQuery table metadata/date ranges, extracts tables to GCS, lists and downloads GCS staging objects,
and writes the local serving file. If `cleanup_gcs_staging=true`, it attempts to delete the current run's temporary
staging objects after a successful export. It then performs a best-effort sweep of temporary objects from failed or debug
runs older than `staging_retention_days`, which defaults to three days and can also be set with
`SERVING_EXPORT_STAGING_RETENTION_DAYS`. Both cleanup phases are best-effort: failures are logged but do not turn an
already published artifact into a failed export.

The stale sweep is deliberately restricted to these roots:

```text
serving/duckdb/staging/export_id=...
serving/duckdb/staging/partition_staging/export_id=...
```

It never lists or deletes `serving/duckdb/staging/partition_cache`, which is the active historical partition cache.
Do not apply a short bucket lifecycle rule to the entire `serving/duckdb/staging/**` prefix.

Failed exports remain available during the retention window. Remove one manually with:

```bash
gcloud storage rm --recursive gs://ztm-analytics-bucket/serving/duckdb/staging/export_id=EXPORT_ID/
gcloud storage rm --recursive gs://ztm-analytics-bucket/serving/duckdb/staging/partition_staging/export_id=EXPORT_ID/
```

Killed exports can also leave local hidden build artifacts under the serving directory. After confirming no serving
export is running, remove them with:

```bash
rm -rf /opt/airflow/serving/.duckdb-tmp-EXPORT_ID \
  /opt/airflow/serving/.validation-tmp-EXPORT_ID \
  /opt/airflow/serving/.ztm.duckdb.EXPORT_ID.tmp* \
  /opt/airflow/serving/ztm.duckdb.meta.json.tmp \
  /opt/airflow/serving/ztm.duckdb.meta.json.restore.tmp
```

The stable DuckDB file and sidecar JSON are not swapped transactionally as one unit. The sidecar is replaced first and
the DuckDB last, so a sidecar-write failure cannot expose a new database. Frontend consumers accept sidecar fields only
when its `export_id` matches `export_metadata`; the DuckDB remains the source of truth for exact consistency. The sidecar
is `ztm.duckdb.meta.json` by default and mirrors the same export summary for operational inspection.

## Operational Notes

- Raw load retries are safe because job IDs are deterministic.
- One malformed GPS coordinate should be filtered at staging and must not fail stop-arrival reconstruction.
- Large ad-hoc BigQuery work should be dry-run and bounded by `maximum_bytes_billed`.
- Default Airflow dbt tests must stay cheap enough for normal cadence; full-history schedule/version tests are manual
  audit work.
- The old `ztm_bq` dataset is gone; rebuild and recovery work should target the v2 datasets.
