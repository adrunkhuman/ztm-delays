# Airflow DAGs

DAGs in this directory are deployed to the Airflow host volume and run inside the Airflow container.

Runtime contract:

- `dbt/` is mounted at `/opt/airflow/dbt`.
- Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, and `google-cloud-storage`.
- Airflow image supports Airflow 3.2 partitioned asset APIs used by these DAGs: `Asset`, asset-event `Metadata`, `CronPartitionTimetable`, `PartitionedAssetTimetable`, and `StartOfDayMapper`.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- Service account can list/read `gs://ztm-analytics-bucket/raw/gps/...` and load/query `ztm-data.ztm_raw`.
- Service account can write `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/query/insert `ztm-data.ztm_raw.raw_gtfs_snapshots`.
- Service account can read `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/load/append GTFS raw tables in `ztm-data.ztm_raw`.
- Service account can create/query/update dbt models in `ztm-data.ztm_stg`, `ztm-data.ztm_int`, and `ztm-data.ztm_marts`.

The GPS raw-load DAG is:

```text
dag_gps_raw_load
```

It runs hourly, loads available poller Parquet files from GCS into `ztm_raw.raw_gps_pings`, and emits the partitioned Airflow asset `raw_gps_date`. The asset means “raw-load attempt for this Warsaw-local GPS date,” not “complete GPS day”; it can emit with `loaded_uri_count = 0`. Day health comes from completeness/status marts.

The GPS warehouse build DAG is:

```text
dag_daily_gps
```

The DAG ID is historical. It is now scheduled by the partitioned `raw_gps_date` asset and rebuilds the emitted partition key's Warsaw-local processing date.

It verifies that at least one GTFS snapshot exists before the processing date, then runs `stg_gps__pings`, `int_ping_trip`, `int_gps_hourly_completeness`, `int_stop_arrivals`, `int_trip_summary`, `fct_trip`, `fct_stop_arrival`, `mart_day_completeness`, `agg_service_coverage`, aggregate marts, and `mart_pipeline_status`. Schedule matching resolves the governing snapshot per GTFS `service_date`: latest loaded snapshot whose Warsaw-local timestamp date is strictly before that service date. Facts publish both the current service date and the prior service date so after-midnight GPS can complete overnight trips without overwriting unrelated partitions. Aggregate/status marts currently rebuild from the collected-history start date through the processing date.

Expected input layout:

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type={bus|tram}/date={processing_date}/hour={00..23}/part-*.parquet
```

Raw-load schedule: `20 * * * *`.

The GTFS polling DAG is:

```text
dag_gtfs_poll
```

It runs hourly, downloads `https://mkuran.pl/gtfs/warsaw.zip`, computes a SHA-256 hash, uploads changed snapshots to `gs://ztm-analytics-bucket/raw/gtfs/`, records metadata in `ztm_raw.raw_gtfs_snapshots`, and emits the Airflow asset `gtfs_snapshot` when the snapshot changed. Unchanged snapshots do not emit an asset event.

The GTFS raw loader DAG is:

```text
dag_gtfs_load
```

It is scheduled by the `gtfs_snapshot` asset. It loads every GTFS snapshot event that triggered the run into raw GTFS BigQuery tables, appends `gtfs_snapshot_id` to each row, runs and tests GTFS staging models, then refreshes and tests archive-safe dimensions, schedule-version models, and current-snapshot convenience lookups for the latest event in the run.

Manual recovery is still available by triggering it with explicit `snapshot_id`, `gcs_path`, and `processing_date` in `dag_run.conf`. Validation is strict: `snapshot_id` must match `YYYY-MM-DDTHH:MM:SSZ_<12 hex>`, `gcs_path` must equal `gs://ztm-analytics-bucket/raw/gtfs/{snapshot_id}.zip`, and `processing_date` must be `YYYY-MM-DD`.

Required ZIP members:

```text
trips.txt
stop_times.txt
stops.txt
shapes.txt
routes.txt
calendar_dates.txt
```

dbt staging models run after raw loading:

```text
stg_gtfs__trips
stg_gtfs__stop_times
stg_gtfs__stops
stg_gtfs__shapes
stg_gtfs__routes
stg_gtfs__calendar_dates
```

GTFS staging exposes all loaded snapshots and carries `gtfs_snapshot_id` as lineage. Schedule matching uses loaded `raw_gtfs_snapshots` history to choose the governing snapshot per `service_date`; the Airflow-provided snapshot ID is used for current convenience lookups and lineage-sensitive model runs, not as one global authority for all matched GPS rows.

Archive-safe dimensions refreshed by `dag_gtfs_load` after each GTFS load:

```text
dim_line
dim_stop_post
dim_stop_group
dim_date
dim_schedule_date
```

Schedule-version models rebuilt from loaded GTFS snapshot history after each GTFS load:

```text
int_gtfs_trip_schedule
int_schedule_version
dim_schedule_version
```

Current-snapshot convenience lookups refreshed for the triggered `gtfs_snapshot_id`:

```text
dim_line_current
dim_stop_post_current
dim_stop_group_current
dim_schedule_date_current
```
