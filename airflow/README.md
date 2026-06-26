# Airflow DAGs

DAGs in this directory are deployed to the Airflow host volume and run inside the Airflow container.

The first production DAG is GPS-only:

```text
dag_daily_gps
```

It loads poller Parquet files from GCS into `ztm_bq.raw_gps_pings`, then runs the existing `stg_gps_pings` dbt model for the processing date.

This intentionally does not handle GTFS, vehicle snapshots, intermediate models, marts, or frontend outputs.
