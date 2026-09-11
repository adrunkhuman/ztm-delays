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

## Monitoring and retention

Set `AIRFLOW_FAILURE_WEBHOOK_URL` for failure callbacks. Inspect task logs, matcher metrics, export validation reports, disk capacity, and poller heartbeat freshness. Exported poller status is a snapshot, not live monitoring. Enable `LOG_BIGQUERY_DBT_JOB_COSTS` for nightly query-cost summaries; attribution is best-effort, not billing enforcement.

Transient matcher tables and intermediate markers default to three-day retention; published markers last 30 days. Stable matcher-input partitions are not transient. Once diagnostics expire, recovery starts from raw inputs and the pinned snapshot.

Successful exports clean temporary GCS objects and inactive local generations after the retention interval, three days by default. Never apply a short lifecycle rule to the entire serving staging prefix: `partition_cache` retains active historical data. Remove failed local build files only after confirming no export is running; do not remove the active catalog or referenced generations.
