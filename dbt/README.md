# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Use Python `3.13` for local dbt commands. The current dbt stack is verified with `dbt-core 1.11.11` and `dbt-bigquery 1.11.3`.

GPS staging and completeness models require `processing_date`. Trip and arrival matching also require the governing `gtfs_snapshot_id`:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select stg_gps__pings --vars '{"processing_date": "YYYY-MM-DD"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select int_ping_trip int_stop_arrivals --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select int_trip_summary fct_trip fct_stop_arrival --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID"}'
```

Archive-safe dimensions rebuild across loaded GTFS snapshots. Current convenience lookups also require the selected `gtfs_snapshot_id`:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date int_gtfs_trip_schedule int_schedule_version dim_schedule_version dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current --vars '{"gtfs_snapshot_id": "SNAPSHOT_ID"}'
```

Historical facts should bake labels from their governing snapshot. `_current` dimensions are present-day convenience surfaces only and must not be used to relabel historical facts.

`fct_trip` and `fct_stop_arrival` overwrite the selected service-date partition for each processing date. Prior-service-date completion from after-midnight GPS is intentionally deferred until the pipeline can rebuild the full prior-day service partition without deleting daytime rows.

Schedule versions are per-line timetable fingerprints derived from governing snapshots across collected history. They intentionally exclude display labels and unstable GTFS identifiers. They are only known from collected snapshots onward, and same-day/intraday schedule changes remain out of scope until #27.

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
