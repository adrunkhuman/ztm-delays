# Operations

Airflow runs a repository-built image on a VPS, using Coolify for manual deployment and external PostgreSQL for metadata. Poller and frontend deploy separately. CI checks code and images; it does not deploy or rebuild data.

## Runtime setup

| Area | Required configuration |
| --- | --- |
| Cloud access | `GOOGLE_APPLICATION_CREDENTIALS` points to a mounted key with access to the configured GCS bucket and BigQuery datasets. |
| Warehouse | Set `GCP_PROJECT`, `GCS_BUCKET`, `BIGQUERY_LOCATION`, and `BIGQUERY_*_DATASET` values consistently in Airflow and dbt. Defaults refer to the existing deployment. |
| Matcher | Set `MATCHER_ENABLED=true` and an isolated `BIGQUERY_MATCHER_STAGING_DATASET`; keep `MATCHER_WORKSPACE_ROOT` writable with room for downloads and spill. |
| Schedule ledger | Create the one-slot `schedule_ledger_writer` pool and bootstrap the ledger before enabling schedule writers. |
| Metadata | Persist PostgreSQL and preserve `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` and `AIRFLOW__CORE__FERNET_KEY`. |
| Logs and login | Persist `/opt/airflow/logs` and `/opt/airflow/auth`; set `AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE=/opt/airflow/auth/passwords.json`. |
| Serving | Airflow needs write access to shared storage; frontend needs read access to the complete export. |

Airflow runs as UID `50000`, group `0`; mounted directories must grant the required access. Keep credentials in runtime configuration, never in images. Preserve the image's isolated matcher environment and command; do not mount over `/opt/airflow/dags`, `/opt/airflow/dbt`, `/opt/airflow/matcher`, `/opt/airflow/matcher-venv`, or their parent.

The [poller](../poller/README.md#container) has its own Tailscale identity and durable spool requirements.

## Poller health feed

Health monitoring is an independent Airflow DAG; it does not depend on nightly reconstruction. Roll it out in order:

1. Deploy the collector and let it create its hourly summaries and `latest.json` heartbeat. It writes locally checkpointed, bounded state (at most 8 MiB per attempt); local `fsync` assumes an SSD/local filesystem, not a network filesystem. A failed upload remains pending for retry. If a collector cap is reached, collection stops loudly without discarding pending statistics.
2. Enable `poller_health` at 25 minutes past each UTC hour. Each check detects degradation sustained for 15 consecutive sufficiently monitored minutes and requires at least 80% of expected poll count per minute; degradation threshold is 0.5. Per-mode results and reports are stored under `health/poller/hourly/` and `health/poller/reports/`. Allow the DAG service account to create the `raw_poller_hourly_health` BigQuery table in the configured raw dataset, as well as its existing GCS read/write access.
3. After the first successful monitor run creates that raw table, install the views once: run `dbt run --select mart_poller_hourly_health mart_poller_daily_health`, then `dbt test --select tag:feed_health`. Do not run a full rebuild. These views read the raw table directly and need no nightly selection or full serving export.
4. Deploy the exporter/frontend changes and publish a normal serving export. They read the optional, allowlisted `health/poller/feed-status.json` into the export metadata sidecar; old heartbeats and exports remain valid. No historical shards need refreshing. The page is a snapshot, not live monitoring. A last evaluated hour ending more than two hours before export is marked stale, even if an old report was recently re-evaluated.

`mart_poller_hourly_health` provides one mode/hour row with Warsaw-local date and hour. `mart_poller_daily_health` summarizes evaluated hours, monitored minutes, degraded intervals/hours, telemetry gaps, warming-up hours, and nullable row counters. Insufficient positive fleet history remains `warming_up`, but demonstrable stale-heavy degradation still takes precedence. Counters stay null when all source values are null. Historical dates before collection are not monitored, and the first collection hour may be partial. Same-weekday/local-hour baselines need at least three samples, so allow about 21 days to warm up; lookback is capped at 28 days. Historical summaries cannot reconstruct rows dropped before monitoring began.

Set `POLLER_HEALTH_WEBHOOK_URL` to an HTTPS endpoint for degradation/recovery events. Once collection is confirmed, sustained missing or insufficient telemetry emits separate `monitoring_gap`/`monitoring_restored` transitions using the same duration control; these do not claim zero GPS feed or feed recovery. Alert delivery and warehouse/snapshot publication run independently after the report is saved, so a BigQuery failure does not suppress saved alerts. Delivery is at-least-once: receivers must deduplicate stable `event_id` values because an ambiguous failure can redeliver. Each hourly alert task retries pending events from the preceding 48 hours. Older pending events remain in their originating report for manual replay with `deliver_alerts(bucket, hour, webhook_url)` before archive expiry. With no webhook, events are acknowledged to the log once; enabling a webhook later does not send them retrospectively. Skipped monitor hours preserve known open incidents but reset minute continuity, so a monitoring gap cannot manufacture recovery.

New raw health tables use 90-day BigQuery partition expiry; existing tables require that expiry to be configured separately. Apply GCS lifecycle rules only to the hourly-summary prefix (at least 28 days) and reports prefix (at least 90 days). Do not expire `feed-status.json` or `latest.json`, or apply expiration to the wider `health/poller/` prefix.

Detection controls are `POLLER_HEALTH_DURATION_MINUTES` (15), `POLLER_HEALTH_COVERAGE_FRACTION` (0.8), `POLLER_HEALTH_THRESHOLD` (0.5), and `POLLER_HEALTH_LOOKBACK_DAYS` (28). Use the same `POLLER_HEALTH_GCS_PREFIX` and `POLLER_HEARTBEAT_GCS_PATH` in collector and monitor when overriding defaults. Fleet baselines are empirical, not timetable-derived; holidays or large schedule changes may need investigation rather than being treated as proof of an upstream outage. No historical fleet-baseline seeding is performed automatically.

If an open fleet incident remains `recovery_unconfirmed` after its usable historical baseline expires, inspect recent fleet counts first. To start a new baseline epoch, trigger `poller_health` for a new, not-yet-evaluated chronological hour with `{"rebaseline_modes": ["bus"]}` (or both modes). This records a distinct administrative `rebaseline` event, not `recovered`, and preserves earlier evidence. It requires an active `low_fleet` or `no_accepted` incident; backfills and unrelated already-evaluated hours are rejected. Fresh fully monitored hours can then establish a new baseline. No facts or historical reports are rewritten.

## Deploy

1. Select a revision whose applicable CI checks passed. In Coolify, use repository-root build context, `/airflow/Dockerfile`, and manual deployment; disable automatic/webhook deployment.
2. Back up PostgreSQL and retain the previous image/settings. Pause scheduling and let running tasks finish before replacing the standalone container.
3. Deploy, then check the revision, mounts, external metadata connection, DAG imports, UI login, and serving permissions. Resume scheduling only after those checks pass.

Deploying code does not correct historical partitions. Reprocess affected dates separately. Image rollback reuses persistent data; an Airflow metadata schema upgrade may also require a compatible database backup.

## Recover a date

Fix the underlying failure before rerunning. Raw load job IDs are deterministic; matcher inputs are immutable; dbt replaces bounded partitions. This makes retries repeatable, not globally transactional: a later fact or mart failure can follow successful matcher publication.

For missing raw GPS loads, rerun `dag_gps_raw_load` for the affected `processing_date`. Then trigger `dag_daily_gps` with explicit configuration:

```json
{"processing_date": "YYYY-MM-DD"}
```

Use the persisted snapshot mapping. Do not substitute the latest GTFS snapshot or manually publish facts before matcher validation. Known archive boundaries require explicit prior-date exclusion; the [historical planner](../airflow/dags/matcher_historical_correction.py) applies the repository's eligibility rules.

Manual `dag_gtfs_load` requires all three fields:

```json
{
  "snapshot_id": "YYYY-MM-DDTHH:MM:SSZ_<12 hex>",
  "gcs_path": "gs://BUCKET/raw/gtfs/SNAPSHOT_ID.zip",
  "processing_date": "YYYY-MM-DD"
}
```

Use the same real snapshot ID in both fields. Recovery involving schedule or lineage changes must refresh audit-tagged evidence models and run their tests, as `dag_weekly_audit` does. Default nightly tests exclude those broader checks.

## Historical corrections

Estimate the affected work and review its date/snapshot scope first. In the Airflow container, with the range, latest published date, and a new plan ID set:

```sh
python /opt/airflow/dags/matcher_historical_correction.py plan \
  --plan-id "$PLAN_ID" --start-date "$START_DATE" --end-date "$END_DATE" \
  --refresh-through-date "$LATEST_PUBLISHED_DATE" --report-json /tmp/correction.json
```

Planning reads BigQuery/GCS and records snapshot pins, input-inventory digests, exclusions, and affected serving dates. It does not mutate warehouse data, but its queries can incur charges. Review the report before executing:

```sh
python /opt/airflow/dags/matcher_historical_correction.py execute \
  --plan-json /tmp/correction.json
```

Execution mutates the warehouse. Dates run sequentially; the controller skips successful runs on resume and stops on a failed existing run. Diagnose and clear only the failed child run, then resume the same plan. Preserve the plan outside disposable container storage if recovery must survive replacement.

After all corrections succeed, one historical serving refresh rebuilds the affected marts and triggers one export. If only that refresh fails, resume it without recomputing successful fact dates. Per-query byte caps do not bound the total cost of a range.

For a fresh warehouse, recover from immutable GCS GPS parts and GTFS ZIPs into new target datasets. Rebuild the snapshot inventory and raw loads, GTFS staging/dimensions, then the processing-date mapping. [Bootstrap the schedule ledger](../dbt/README.md#schedule-ledger) in bounded batches and validate its completeness before publishing versions. Reconstruct dates and validate facts/marts before exposing the export. Never use ledger `--full-refresh` or publish a partial bootstrap.

## Serving publication

`dag_serving_export` builds and validates a candidate before replacing `ztm.duckdb`. Manual retries need a **new `export_id`**: deterministic BigQuery extract job IDs cannot be reused safely after an attempt.

After a manual mart rebuild, supply every changed serving date:

```json
{
  "export_id": "recovery-UNIQUE_ID",
  "changed_partition_dates": ["YYYY-MM-DD"]
}
```

Omitting the date list requests a full refresh and can be expensive. Structural or semantic validation failures keep the previous catalog active. Incomplete but internally consistent source days produce warnings recorded in the metadata sidecar.

The partitioned store is enabled with `SERVING_EXPORT_PARTITIONED_STORE=true`; the source default is `false`. It retains date partitions under `parquet/` and publishes a small DuckDB catalog over them. The first run downloads all retained partitions.

Mount the same host directory at `/opt/airflow/serving` and `/serving` in Airflow, and at `/serving` read-only in frontend. Set:

```text
SERVING_EXPORT_DIR=/opt/airflow/serving
SERVING_EXPORT_PARTITIONED_STORE_VIEW_ROOT=/serving
ZTM_DUCKDB_PATH=/serving/ztm.duckdb
```

`ZTM_DUCKDB_PATH` belongs to the frontend; the other settings belong to Airflow. Absolute Parquet paths are stored in the catalog, so both containers must resolve `/serving` to the same files. Back up or restore the catalog, sidecar, and referenced Parquet generations together.

The catalog is the publication point. Sidecar and database replacement are separate; consumers check matching export IDs. Old Parquet generations remain available for readers holding the previous catalog.

Monitor the whole serving filesystem, not just the DuckDB file. Downloads enforce a free-space reserve. To return to a materialized export, disable partitioned mode and publish a fresh export within its size limits; keep Parquet until replacement succeeds.

## Route maps

`dag_monthly_route_map` runs at 12:00 Warsaw time on the 2nd and builds the previous month. `check_month_final` first requires `fct_expected_stop_event` for every expected service date of that month. Dates before the archive start, and known excluded dates, are not expected. It also requires `mart_pipeline_status` for the 1st of the next month, which the nightly run writes only after republishing the month's last day. While anything is missing, the check retries hourly for 12 hours and then fails, listing what is missing.

To backfill or rebuild a month, trigger the DAG with:

```json
{"month": "2026-07"}
```

A rebuild never exposes a partial month. It swaps directories with two renames, so the month is briefly absent between them, and the frontend returns 404 for that instant. Open map pages then ask for a reload rather than show stale corridors. Historical corrections do not update published maps; rebuild the affected months.

| Item | Value |
| --- | --- |
| Output | `$SERVING_EXPORT_DIR/maps/YYYY-MM/`, read by the frontend as `maps/` beside `ZTM_DUCKDB_PATH` |
| BigQuery | Two queries of about 4 GB each, capped at 10 GB billed |
| Runtime | About 3 minutes; peak memory about 3.5 GB |
| Failure checks | Coverage dates, traversal reconciliation, and at least 95% of daytime traversals mapped |

The task's `report.json` records exclusions and coverage per period. Mini-map street backgrounds are fixed assets. Regenerate them with `airflow/scripts/build_route_map_backgrounds.py` only if the frames or styling change.

## Trip planner

`dag_planner_train` runs on Sundays at 13:00 Warsaw time: `train_model`, then `build_footpaths`. They run one after the other to limit peak memory, and `build_footpaths` runs even when training fails (`all_done`). `dag_planner_score` runs daily at 06:30, after the 04:00 warehouse run has published yesterday. Its recent conditions end on the latest published day, so a late warehouse run makes them a day older but never leaves a gap. The planner tab appears once `planner/planner.duckdb` exists.

On first deployment, trigger `dag_planner_train`, confirm promotion in its log, then trigger `dag_planner_score`. Scoring fails until a model is promoted. The footpath build holds the regional OSM graph in memory, outside DuckDB's limit; check its peak on the first run.

| Item | Value |
| --- | --- |
| Output | `$SERVING_EXPORT_DIR/planner/planner.duckdb`, read by the frontend from `planner/` beside `ZTM_DUCKDB_PATH` |
| Models | `gs://$GCS_BUCKET/planner/models/<version>/`; `planner/models/current.json` names the promoted version |
| Footpaths | `gs://$GCS_BUCKET/planner/footpaths/footpaths.parquet`, rebuilt weekly and turned into walking times by scoring |
| Workspace | `PLANNER_WORKSPACE_ROOT` (default `/opt/airflow/planner-work`), on disk rather than tmpfs, with about 5 GB free. Training clears its folder after every run; scoring keeps only the current bundle. |
| Command | `PLANNER_COMMAND` (image default `/opt/airflow/planner-venv/bin/ztm-planner`). Global flags go before the subcommand, e.g. `ztm-planner --memory-limit 1000MB`. `PLANNER_TIMEOUT_SECONDS` defaults to 4 hours. Progress and errors stream into the task log. |
| BigQuery | Training about 16 GB billed per week; scoring about 0.5 GB per night. Each query is capped at 20 GB. |
| Memory | Training peaks near 3 GB, scoring below 2 GB. Both use CPU cores minus two at lowered priority. |

`build_footpaths` downloads the latest GTFS ZIP and [Geofabrik's Mazowieckie PBF](https://download.geofabrik.de/europe/poland/mazowieckie-latest.osm.pbf); allow outbound HTTPS to Geofabrik. The URL is fixed in `planner_pipeline.py`. A failed build keeps the previous GCS file in use and may leave files in `$PLANNER_WORKSPACE_ROOT/footpaths/`. With no file at all, scoring logs a warning and estimates every walk; an error downloading an existing file fails scoring instead. To refresh only walks, rerun `build_footpaths`, then `publish_planner`.

A failed promotion gate fails `train_model`, logs the held-out errors and keeps the previous model; `training_complete` then fails too, even if footpaths succeed. To roll back, point `current.json` at an earlier version (`{"version": "..."}`) and rerun `dag_planner_score`. A failed scoring run leaves the previous artifact, which still covers the next six days.

The frontend reads the artifact locally. Each process caches the networks of two service days, keyed by the artifact's `build_id`; building one takes about 1.5 s.

## Monitoring and retention

Set `AIRFLOW_FAILURE_WEBHOOK_URL` for failure callbacks. Inspect task logs, matcher metrics, export validation reports, disk capacity, and poller heartbeat freshness. Exported poller status is a snapshot, not live monitoring. Enable `LOG_BIGQUERY_DBT_JOB_COSTS` for nightly query-cost summaries; attribution is best-effort, not billing enforcement.

Transient matcher tables and intermediate markers default to three-day retention; published markers last 30 days. Stable matcher-input partitions are not transient. Once diagnostics expire, recovery starts from raw inputs and the pinned snapshot.

Successful exports clean temporary GCS objects and inactive local generations after the retention interval, three days by default. Never apply a short lifecycle rule to the entire serving staging prefix: `partition_cache` retains active historical data. Remove failed local build files only after confirming no export is running; do not remove the active catalog or referenced generations.
