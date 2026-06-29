# Airflow DAGs

DAGs in this directory are deployed to the Airflow host volume and run inside the Airflow container.

Runtime contract:

- `dbt/` is mounted at `/opt/airflow/dbt`.
- Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, and `google-cloud-storage`.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- Service account can list/read `gs://ztm-analytics-bucket/raw/gps/...` and load/query `ztm-data.ztm_raw`.
- Service account can write `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/query/insert `ztm-data.ztm_raw.raw_gtfs_snapshots`.
- Service account can read `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/load/append GTFS raw tables in `ztm-data.ztm_raw`.
- Service account can create/query/update dbt models in `ztm-data.ztm_stg`, `ztm-data.ztm_int`, and `ztm-data.ztm_marts`.

The GPS processing DAG is:

```text
dag_daily_gps
```

The DAG ID is historical; it now runs hourly and rebuilds the data interval's Warsaw-local processing date.

It loads poller Parquet files from GCS into `ztm_raw.raw_gps_pings`, selects the governing GTFS snapshot, then runs `stg_gps__pings`, `int_ping_trip`, `int_gps_hourly_completeness`, and `int_stop_arrivals` for the processing date. The governing snapshot is the latest `ztm_raw.raw_gtfs_snapshots.snapshot_timestamp` whose Warsaw-local date is strictly before the GPS processing date; the DAG fails if no such snapshot exists. This is a metadata lookup only; `dag_daily_gps` does not wait for the selected snapshot's raw GTFS tables or dimensions to be loaded.

Expected input layout:

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type={bus|tram}/date={processing_date}/hour={00..23}/part-*.parquet
```

Schedule: `20 * * * *`.

The GTFS polling DAG is:

```text
dag_gtfs_poll
```

It runs hourly, downloads `https://mkuran.pl/gtfs/warsaw.zip`, computes a SHA-256 hash, uploads changed snapshots to `gs://ztm-analytics-bucket/raw/gtfs/`, records metadata in `ztm_raw.raw_gtfs_snapshots`, and triggers `dag_gtfs_load` when the snapshot changed.

The GTFS raw loader DAG is:

```text
dag_gtfs_load
```

It loads the triggered GTFS snapshot ZIP into raw GTFS BigQuery tables, appends `gtfs_snapshot_id` to each row, runs and tests GTFS staging models, then refreshes and tests archive-safe dimensions plus current-snapshot convenience lookups. It is unscheduled and triggered by `dag_gtfs_poll` with immutable `snapshot_id`, `gcs_path`, and `processing_date` in `dag_run.conf`.

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

GTFS staging exposes all loaded snapshots and carries `gtfs_snapshot_id` as lineage. Downstream intermediate models use the Airflow-provided governing snapshot ID to pin schedule joins for a processing date.

Archive-safe dimensions refreshed by `dag_gtfs_load` after each GTFS load:

```text
dim_line
dim_stop_post
dim_stop_group
dim_date
dim_schedule_date
```

Current-snapshot convenience lookups refreshed for the triggered `gtfs_snapshot_id`:

```text
dim_line_current
dim_stop_post_current
dim_stop_group_current
dim_schedule_date_current
```
