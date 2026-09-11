# Airflow

Orchestrates collection, reconstruction, warehouse modelling, and serving publication. Raw ingestion and nightly processing have separate schedules; an hourly load does not imply a complete GPS day.

## DAGs

| DAG | Trigger | Work |
| --- | --- | --- |
| `dag_gtfs_poll` | Hourly | Store changed GTFS ZIPs; emit `gtfs_snapshot`. |
| `dag_gtfs_load` | `gtfs_snapshot` | Load the exact snapshot; rebuild staging, dimensions, and schedule versions. |
| `dag_gps_raw_load` | Hourly | Load available GPS parts; emit `raw_gps_date`. |
| `dag_daily_gps` | Nightly, 04:00 Warsaw | Reconstruct the previous GPS date; publish facts, coverage, and marts; emit `gps_models_date`. |
| `dag_serving_export` | `gps_models_date` or manual | Validate and publish the frontend export. |
| `dag_weekly_audit` | Weekly | Refresh audit evidence and run audit-tagged dbt tests. |
| `dag_historical_serving_refresh` | Historical correction controller | Consolidate serving rebuilds and trigger one export after corrections. |

The nightly DAG selects the persisted GTFS snapshot, validates staging, invokes the matcher, and promotes its outputs before dbt publishes service-date facts. Airflow checks schemas, hashes, lineage, row uniqueness, mode coverage, and resource use. The four stable matcher-input partitions are replaced in one BigQuery transaction; downstream fact and mart rebuilds are separate steps.

Schedule-writing tasks share the one-slot `schedule_ledger_writer` pool. Reconstruction requires `MATCHER_ENABLED=true`, an isolated `BIGQUERY_MATCHER_STAGING_DATASET`, and a writable matcher workspace. [Operations](../docs/operations.md) covers initial setup, deployment, and recovery.

## Image

The [Dockerfile](Dockerfile) builds from the repository root and runs `airflow standalone` with external PostgreSQL metadata. DAGs, dbt, and matcher code are baked into the image. Matcher dependencies use a separate locked environment; tasks do not download packages at runtime.

CI builds and checks the image. Deployment is manual and does not run warehouse models. Do not mount host source over image-owned directories.

## Checks

From the repository root, without cloud credentials:

```sh
uvx --with tzdata==2026.3 --with duckdb==1.5.4 --with pyarrow==25.0.0 \
  --with jinja2==3.1.6 pytest==9.1.1 airflow/tests
```

The tests exercise DAG boundaries and helpers, not a running scheduler. To check the image:

```sh
docker build --platform linux/amd64 -f airflow/Dockerfile -t ztm-airflow:local .
docker run --rm --network none --entrypoint python \
  -v "$PWD/.github/scripts/smoke_airflow_image.py:/tmp/smoke_airflow_image.py:ro" \
  ztm-airflow:local /tmp/smoke_airflow_image.py
```
