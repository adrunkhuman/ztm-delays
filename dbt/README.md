# ZTM dbt Project

This dbt project transforms BigQuery raw tables for the ZTM pipeline.

Use Python `3.13` for local dbt commands. The current dbt stack is verified with `dbt-core 1.11.11` and `dbt-bigquery 1.11.3`.

GPS staging and completeness models require `processing_date`. The Python matcher owns trip and arrival reconstruction; dbt enriches its stable inputs and publishes current/prior service-date facts:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select stg_gps__pings --vars '{"processing_date": "YYYY-MM-DD"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select fct_trip fct_stop_arrival fct_expected_stop_event --vars '{"processing_date": "YYYY-MM-DD", "gtfs_snapshot_id": "SNAPSHOT_ID", "publish_service_date": "SERVICE_DATE"}'

uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select mart_day_completeness agg_service_coverage mart_pipeline_status --vars '{"processing_date": "YYYY-MM-DD", "aggregation_start_date": "YYYY-MM-DD"}'
```

Archive-safe dimensions rebuild across loaded GTFS snapshots. Current convenience lookups also require the selected `gtfs_snapshot_id`:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt run --select dim_line dim_stop_post dim_stop_group dim_date dim_schedule_date int_gtfs_trip_schedule int_schedule_version dim_schedule_version dim_line_current dim_stop_post_current dim_stop_group_current dim_schedule_date_current --vars '{"gtfs_snapshot_id": "SNAPSHOT_ID"}'
```

Historical facts bake labels from the selected snapshot used for their rebuild. `_current` dimensions are present-day convenience surfaces only and must not be used to relabel historical facts.

The three facts overwrite `publish_service_date`, defaulting to `processing_date`. A normal matcher `gps_date = processing_date` artifact already combines source GPS dates `D-1` and `D`; dbt selects that one artifact by exact `gps_date` when publishing both service-date partitions. `source_gps_date` keeps direct-arrival lineage. Outage-boundary runs explicitly use current-only input rather than reading an excluded prior GPS date.

Schedule versions are per-line timetable fingerprints derived from selected snapshots across collected history. They intentionally exclude display labels and unstable GTFS identifiers. Nightly runs use the latest built GTFS snapshot and republish the prior service date, so late corrections for yesterday are picked up by the next run.

Scheduled GTFS timestamps use one Warsaw wall-clock macro for service date plus GTFS seconds. It normalizes spring-forward gaps and chooses the first fall-back occurrence, matching the Python matcher.

`mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status` incrementally replace the inclusive `[aggregation_start_date, processing_date]` partitions. `mart_day_completeness` summarizes raw GPS presence. `agg_service_coverage` compares scheduled trips with complete/partial trip facts. `mart_pipeline_status` combines ingestion completeness, trip quality, settled service coverage, stop-arrival counts, and GTFS freshness.

## Test Tiers

Default Airflow runs exclude the expensive full-history tests on `int_gtfs_trip_schedule` and `int_schedule_version`. Run them manually before schedule/matcher/audit-sensitive releases:

```bash
uvx --python 3.13 --from dbt-core --with dbt-bigquery dbt test --select int_gtfs_trip_schedule int_schedule_version --indirect-selection cautious --exclude test_type:unit --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

That manual selector uses singular contract tests for required fields and accepted values instead of repeated generic column tests over the expensive schedule views.

For fact/status audits, pass `aggregation_start_date` and `publish_service_date` explicitly and keep those vars aligned with the rebuild window. Do not run unbounded full-history tests casually.

The local `profiles.yml` uses environment variables for BigQuery connection settings and credentials.

In Airflow, `GOOGLE_APPLICATION_CREDENTIALS` defaults to `/opt/airflow/gcp-key.json` if the environment variable is not set explicitly.
