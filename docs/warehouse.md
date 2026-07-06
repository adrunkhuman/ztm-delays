# Warehouse

The warehouse is BigQuery-first. Raw inputs live in GCS, raw/staging/intermediate/mart layers live in separate datasets, and the frontend reads a serving DuckDB export built from selected marts.

## Layers

| Layer | Default dataset | Purpose |
| --- | --- | --- |
| Raw | `ztm_raw` | Rebuildable BigQuery loads from immutable GCS objects. |
| Staging | `ztm_stg` | Type cleanup, renaming, dedupe, and structural guards. |
| Intermediate | `ztm_int` | Reusable trip, schedule, and stop-arrival reconstruction. |
| Marts | `ztm_marts` | Frontend/export-facing dimensions, facts, aggregates, and status. |

Airflow and dbt use the same runtime env names: `GCP_PROJECT`, `BIGQUERY_RAW_DATASET`, `BIGQUERY_STG_DATASET`, `BIGQUERY_INT_DATASET`, `BIGQUERY_MARTS_DATASET`, and `BIGQUERY_LOCATION`. Defaults match the current VPS.

dbt uses `generate_schema_name` to route model layers to exact dataset names. Raw sources use `BIGQUERY_RAW_DATASET`, defaulting to `ztm_raw`.

## Naming

Staging uses `stg_<source>__<entity>`, for example `stg_gps__pings` and `stg_gtfs__stop_times`.

Intermediate models use `int_<purpose>`. Marts use `dim_`, `fct_`, `agg_`, or `mart_`. Raw loader tables keep source names such as `raw_gps_pings` and `raw_gtfs_trips`.

Archive-safe dimensions:

- `dim_line`
- `dim_stop_group`
- `dim_stop_post`
- `dim_date`
- `dim_schedule_date`
- `dim_schedule_version`

Current convenience dimensions:

- `dim_line_current`
- `dim_stop_group_current`
- `dim_stop_post_current`
- `dim_schedule_date_current`

Use `_current` tables for present-day filters/maps only. Do not join historical facts or aggregates to `_current` tables for labels.

## Lineage Rules

GTFS staging spans all loaded snapshots. It exposes `gtfs_snapshot_id`; downstream models must choose the governing snapshot explicitly.

The governing snapshot for a GTFS `service_date` is the latest loaded snapshot whose Warsaw-local snapshot date is strictly before that service date. Same-day snapshots do not govern that same service day.

Intermediate schedule joins must include `gtfs_snapshot_id` and carry it forward. Historical facts must bake labels from the governing snapshot at build time.

Public `line` is not a durable historical entity by itself. The same line label can later point to different stop sets or schedules. Compare history through GTFS snapshot or schedule-version lineage.

## Staging

`stg_gps__pings` processes one Warsaw-local `processing_date`. It deduplicates by `vehicle_number` and `gps_time`, normalizes numeric identifiers, and drops structurally impossible coordinates before geography functions run.

GTFS staging keeps snapshot lineage and does not filter to one snapshot. Downstream models decide which snapshot governs each date.

Structural garbage should be removed in staging only when it cannot be analyzed safely. Suspicious but analyzable behavior belongs in quality flags, not hard failures.

## Dimensions

`dim_line`, `dim_stop_group`, and `dim_stop_post` are date-ranged dictionaries. Consecutive governing snapshots collapse into one row when display attributes do not change. If an entity disappears, the prior row closes the day before disappearance.

`dim_stop_group` groups stops by the first four characters of `stop_id`. Warsaw bus/tram posts usually use six-digit IDs, but the feed also includes metro, rail, depot, entrance, and platform records. The durable grouping rule is the prefix, not universal six-digit shape.

`dim_date` is calendar-only. `day_type` is weekday/weekend, and `is_holiday` is the Polish public-holiday flag.

`dim_schedule_date` is schedule-aware. It exposes `schedule_day_type` from active GTFS service IDs in the governing snapshot. A calendar weekday can have holiday/Sunday service when GTFS says so.

Intermediate GPS/trip models still carry calendar `day_type` from staging. Do not use it as a schedule-pattern field; use `schedule_day_type` from `dim_schedule_date` or baked fact columns.

`route_long_name` is currently null in line dimensions because the raw route loader does not retain that optional field.

## Schedule Versions

`int_gtfs_trip_schedule` denormalizes scheduled trips under each service date's governing snapshot. It carries both `processing_date` and `service_date` because overnight GPS matching can use the prior service date.

`int_schedule_version` and `dim_schedule_version` define the "since last timetable change" baseline. Grain: `line`, `direction_id`, and `schedule_day_type` over a consecutive validity range.

The timetable fingerprint uses scheduled stop/time content only. It excludes snapshot IDs, trip IDs, service IDs, labels, and other display fields so republishing the same timetable does not create false version changes.

`schedule_version_id` changes when the timetable fingerprint changes. If a timetable changes away and later returns, it becomes a new version period because the ID includes `valid_from_date`.

The mkuran GTFS feed is a rolling window. Schedule versions are known only from collected snapshots onward. A null `valid_to_date` means no later collected change is known.

## Trip Facts

`int_trip_summary` is one observed vehicle trip candidate per `gps_date`, `gtfs_snapshot_id`, `service_date`, `trip_id`, and `vehicle_number`. It starts from reconstructed stop arrivals. Pings are diagnostics, not standalone trip facts.

`fct_trip` is partitioned by `service_date`, requires a partition filter, and is clustered by `line` and `direction_id`. Airflow publishes both the current and prior service-date partitions for each GPS processing date so overnight trips can complete without deleting prior-day daytime rows.

`fct_trip` carries archive-safe labels from the governing snapshot: mode, route short name, trip headsign, origin stop, and destination stop.

Trip quality policy:

- `complete`: default for strict analytics.
- `partial`: acceptable for exploration and drill-down.
- `broken`: debug only; likely wrong or too incomplete for analytics.

Quality thresholds are provisional constants in `int_trip_summary`. Change them only with real-data validation.

## Stop Arrivals

`fct_stop_arrival` is one detected scheduled stop arrival per `gtfs_snapshot_id`, `service_date`, `trip_id`, `vehicle_number`, and `stop_sequence`. It is built from `int_stop_arrivals` and inherits trip lineage from `fct_trip`.

The fact carries labels directly: stop name, stop coordinates, stop-group name, route short name, mode, and trip headsign. Historical pages should render those baked labels.

`delay_seconds = actual_arrival_time - scheduled_arrival_time`. Positive is late; negative is early.

`hour_bracket` is the Warsaw-local scheduled-arrival hour. Leaderboards and time-of-day charts should use scheduled hour, not actual-arrival hour.

The table is partitioned by `service_date`, requires a partition filter, and is clustered by `line`, `stop_group_id`, and `hour_bracket`.

## Completeness

Trip facts are the analytics validity grain. A day can be incomplete while individual `complete` trips remain valid.

`mart_day_completeness` summarizes raw GPS ingestion by `gps_date` and mode. It is operational ingestion coverage, not schedule-aware service coverage.

`agg_service_coverage` summarizes scheduled bus/tram service observed by line, direction, headsign, scheduled-start date, and scheduled hour. Expected trips come from `int_gtfs_trip_schedule`; observed trips come from `int_trip_summary` rows with `complete` or `partial` quality. `broken` rows are excluded.

`agg_service_coverage` emits rows only for scheduled bus/tram service hours. No row means no scheduled service. A row with `service_coverage_ratio = 0` means scheduled service was not observed.

`mart_pipeline_status` is the historical archive-health surface by Warsaw-local operational date and mode. Near-real-time poller liveness comes from the private heartbeat captured by the serving export, not from this mart.

## Aggregates

Aggregate marts are serving accelerators over `fct_stop_arrival`. They use `trip_quality = 'complete'` rows and can be rebuilt from detail.

Period types:

- `month`: calendar month. Month rows use the latest schedule version observed in that month for each line/direction/schedule-day type.
- `schedule_version`: one timetable-version period from `dim_schedule_version`.

Period rows expose `source_start_date`, `source_end_date`, and `is_partial_period`. Consumers should label or exclude partial periods when comparing complete windows.

Aggregate day classes:

- `day_type`: calendar weekday/weekend.
- `weekday`: Monday through Sunday.
- `schedule_day_type`: GTFS-derived service pattern.

Delay stats use the same columns across aggregate marts: `n`, `mean_delay_seconds`, `p10_delay_seconds`, `median_delay_seconds`, `p50_delay_seconds`, `p90_delay_seconds`, `stddev_delay_seconds`, `on_time_rate`, and fixed histogram buckets.

Main aggregate roles:

- `agg_line_stop_period`: line/stop/hour/period axis.
- `agg_stop_period`: stop/line/hour/period axis.
- `agg_time_period`: network time-of-day axis.
- `agg_line_daily`: daily line trend surface.

Aggregate rows carry display labels and `gtfs_snapshot_ids` lineage from the source facts. Frontend code must not relabel historical aggregates through `_current` dimensions.

## Serving Export

`dag_serving_export` is manual. It exports a fixed allowlist of marts to GCS Parquet, downloads them in the Airflow worker, builds derived DuckDB serving tables, validates guardrails, and atomically swaps the stable DuckDB file.

The export is not a generic mirror of `ztm_marts`. It contains only current frontend source tables, derived DuckDB tables, `export_metadata`, and `export_table_stats`.

Changing the serving surface means updating the DAG allowlist, derived SQL, tests, and serving contract together.

## Tests And Cost

`error` tests are for structural invariants: uniqueness at declared grain, not-null keys, and enum accepted values. Distributional checks, low coverage, suspicious delays, and quality thresholds should be warnings or model columns unless the data is structurally unusable.

Large partitioned models use `insert_overwrite` with bounded static partitions. Rerunning the same window replaces partitions and should not duplicate rows.

Large partitioned tables should require partition filters. dbt models must filter upstream by the partition they overwrite.

Final projections should list columns explicitly. Avoid `select *` in outputs.

The dbt BigQuery profile sets `maximum_bytes_billed`, defaulting to 100 GB per query. Large backfills should be dry-run first.

Default Airflow paths skip known expensive schedule/version tests and broad aggregate tests. Full-history schedule/version tests, broad aggregate tests, broader contract audits, and serving export schema audits are manual jobs.

Nightly GPS runs log BigQuery dbt job cost metadata from `INFORMATION_SCHEMA.JOBS_BY_USER`. Treat the initial 100 GiB warning threshold as an operational signal until calibrated.

## Current Cutover

The v2 datasets are built in parallel with old `ztm_bq`. Keep `ztm_bq` until `ztm_raw`, `ztm_stg`, `ztm_int`, and `ztm_marts` are validated.
