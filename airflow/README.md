# Airflow DAGs

DAGs in this directory are deployed to the Airflow host volume and run inside the Airflow container.

Runtime contract:

- `dbt/` is mounted at `/opt/airflow/dbt`.
- Airflow image includes `dbt`, `dbt-bigquery`, `google-cloud-bigquery`, and `google-cloud-storage`.
- `GOOGLE_APPLICATION_CREDENTIALS` points to the mounted GCP service account key.
- Service account can list/read `gs://ztm-analytics-bucket/raw/gps/...` and load/query `ztm-data.ztm_bq`.

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
