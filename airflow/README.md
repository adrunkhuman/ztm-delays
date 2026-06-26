# Airflow DAGs

DAGs in this directory are deployed to the Airflow host volume and run inside the Airflow container.

Runtime contract:

- `dbt/` is mounted at `/opt/airflow/dbt`.
- Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, and `google-cloud-storage`.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- Service account can list/read `gs://ztm-analytics-bucket/raw/gps/...` and load/query `ztm-data.ztm_bq`.
- Service account can write `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/query/insert `ztm-data.ztm_bq.raw_gtfs_snapshots`.
- Service account can read `gs://ztm-analytics-bucket/raw/gtfs/*.zip` and create/load/append GTFS raw tables in `ztm-data.ztm_bq`.

The first production DAG is GPS-only:

```text
dag_daily_gps
```

It loads poller Parquet files from GCS into `ztm_bq.raw_gps_pings`, then runs the existing `stg_gps_pings` dbt model for the processing date.

Expected input layout:

```text
gs://ztm-analytics-bucket/raw/gps/vehicle_type={bus|tram}/date={{ ds }}/hour={00..23}/part-*.parquet
```

Schedule: `0 5 * * *`.

This intentionally does not handle GTFS, vehicle snapshots, intermediate models, marts, or frontend outputs.

The GTFS polling DAG is:

```text
dag_gtfs_poll
```

It runs hourly, downloads `https://mkuran.pl/gtfs/warsaw.zip`, computes a SHA-256 hash, uploads changed snapshots to `gs://ztm-analytics-bucket/raw/gtfs/`, and records metadata in `ztm_bq.raw_gtfs_snapshots`.

This intentionally does not parse or load GTFS text files into raw GTFS BigQuery tables yet.

The GTFS raw loader DAG is:

```text
dag_gtfs_load
```

It loads the latest `raw_gtfs_snapshots` ZIP into raw GTFS BigQuery tables and appends `gtfs_snapshot_id` to each row. It is unscheduled and intended to be triggered after `dag_gtfs_poll` records a changed snapshot.

Required ZIP members:

```text
trips.txt
stop_times.txt
stops.txt
shapes.txt
routes.txt
calendar_dates.txt
```

This intentionally does not add dbt GTFS staging models yet.
