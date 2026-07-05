# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Use Python `3.13` for local dbt commands. The current dbt stack is verified with `dbt-core 1.11.11` and `dbt-bigquery 1.11.3`.

GPS staging and completeness models require `processing_date`. Trip and arrival matching require `gtfs_snapshot_id` for lineage/current lookup context, while schedule joins resolve governing snapshots per `service_date` from loaded `raw_gtfs_snapshots` history:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select stg_gps__pings --vars '{"processing_date": "YYYY-MM-DD"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select int_ping_trip int_stop_arrivals --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select int_trip_summary fct_trip fct_stop_arrival --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select agg_line_stop_period agg_stop_period agg_time_period agg_line_daily --vars '{"processing_date": "YYYY-MM-DD", "aggregation_start_date": "YYYY-MM-DD"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select mart_day_completeness agg_service_coverage mart_pipeline_status --vars '{"processing_date": "YYYY-MM-DD", "aggregation_start_date": "YYYY-MM-DD"}'
```

Archive-safe dimensions rebuild across loaded GTFS snapshots. Current convenience lookups also require the selected `gtfs_snapshot_id`:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date int_gtfs_trip_schedule int_schedule_version dim_schedule_version dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current --vars '{"gtfs_snapshot_id": "SNAPSHOT_ID"}'
```

Historical facts should bake labels from their governing snapshot. `_current` dimensions are present-day convenience surfaces only and must not be used to relabel historical facts.

`fct_trip` and `fct_stop_arrival` overwrite `publish_service_date`, defaulting to `processing_date`. Production publishes both `processing_date` and `processing_date - 1` so after-midnight GPS can complete overnight trips without deleting daytime rows.

Schedule versions are per-line timetable fingerprints derived from governing snapshots across collected history. They intentionally exclude display labels and unstable GTFS identifiers. They are only known from collected snapshots onward, and same-day/intraday schedule changes remain out of scope until #27.

Period aggregate marts are table materializations over the inclusive `[aggregation_start_date, processing_date]` source window. Set `aggregation_start_date` for bounded rebuilds; if omitted, the models rebuild all available fact history up to `processing_date`. A bounded run replaces the period aggregate tables with only that source window, so use `source_start_date`, `source_end_date`, and `is_partial_period` when serving bounded outputs.

`mart_day_completeness` incrementally replaces the `processing_date` partition and summarizes raw GPS presence by GPS date and mode. `agg_service_coverage`, `agg_line_daily`, and `mart_pipeline_status` incrementally replace the inclusive `[aggregation_start_date, processing_date]` partitions; normal Airflow runs pass the prior date as `aggregation_start_date` because observed overnight trips can land on the next GPS date and facts publish both current and prior service dates. A 2026-06-25-through-current lag check found complete/partial `int_trip_summary` rows only at same-day and next-day lag, supporting the two-day normal coverage window. `agg_service_coverage` compares governed scheduled trips to complete/partial observed trip candidates from `int_trip_summary` by scheduled service hour and partitions rows by `scheduled_start_date`; overnight rows can keep the prior GTFS `service_date`. `mart_pipeline_status` combines completeness, matching, trip quality, settled service coverage, stop-arrival output counts, and GTFS freshness by operational status date and mode; its `service_date` field is aligned to GPS processing date and scheduled-start date, not necessarily GTFS service_date for overnight trips. Treat `mart_pipeline_status` rows as generated operational reports for their partition, not as a table that refreshes every historical row with the latest global status every night.

## Test Tiers

Default Airflow runs exclude the expensive full-history tests on `int_gtfs_trip_schedule` and `int_schedule_version`. Run them manually before schedule/matcher/audit-sensitive releases:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt test --select int_gtfs_trip_schedule int_schedule_version --indirect-selection cautious --exclude test_type:unit --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

That manual selector uses singular contract tests for required fields and accepted values instead of repeated generic column tests over the expensive schedule views.

Default Airflow runs also exclude broad aggregate mart tests over `agg_line_stop_period`, `agg_stop_period`, `agg_time_period`, and `agg_line_daily`. Nightly Airflow still tests `mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status`; only the four broad serving aggregate tests moved to manual audits. For aggregate/fact audits, pass `aggregation_start_date` and `publish_service_date` explicitly and keep those vars aligned with the aggregate mart build window. Do not run unbounded full-history tests casually.

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
