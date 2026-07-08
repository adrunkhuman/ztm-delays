# Warehouse

The warehouse turns immutable ZTM inputs into an archive that can be rebuilt, audited, and exported for the frontend. Keep it boring: raw data stays recoverable, historical labels are baked at build time, and expensive work is explicit.

## Current Shape

Production BigQuery datasets:

| Dataset | Role |
| --- | --- |
| `ztm_raw` | Raw BigQuery loads from GCS. |
| `ztm_stg` | Cleaned and typed staging models. |
| `ztm_int` | Reusable schedule, trip, and arrival reconstruction. |
| `ztm_marts` | Facts, dimensions, aggregates, status, and export inputs. |

The old `ztm_bq` dataset has been removed.

Airflow and dbt share the same runtime env names: `GCP_PROJECT`, `BIGQUERY_RAW_DATASET`, `BIGQUERY_STG_DATASET`, `BIGQUERY_INT_DATASET`, `BIGQUERY_MARTS_DATASET`, and `BIGQUERY_LOCATION`. Defaults match the current VPS.

dbt routes model layers through `generate_schema_name`. Raw sources use `BIGQUERY_RAW_DATASET`, defaulting to `ztm_raw`.

## Hard Rules

- Raw GCS objects are the recovery source. BigQuery raw tables are reloadable.
- GTFS history is snapshot-based. Never silently match old GPS against the newest GTFS snapshot.
- Historical facts and aggregates carry their own labels. Do not relabel history through `_current` dimensions.
- Public `line` is not a durable historical entity by itself. Use snapshot or schedule-version lineage for historical comparison.
- Large tables require partition filters. Rebuild bounded partitions, not whole history.
- Normal Airflow paths stay cheap. Broad audits are manual jobs.

## Inputs

Raw GPS objects use the configured `GCS_BUCKET` and `RAW_GPS_PREFIX`, defaulting to `gs://ztm-analytics-bucket/raw/gps`.

Raw GTFS snapshots use `RAW_GTFS_PREFIX`, defaulting to `raw/gtfs`. Snapshot IDs are immutable: `{snapshot_timestamp}_{sha256[:12]}`.

`dag_gtfs_poll` stores changed GTFS ZIPs and emits the snapshot asset. `dag_gtfs_load` loads raw GTFS tables and rebuilds GTFS staging/dimensions. `dag_daily_gps` loads raw GPS parts and rebuilds one GPS processing date plus its required prior-service-date facts.

## Snapshot Semantics

Nightly GPS rebuilds use the latest dimension-built GTFS snapshot available at rebuild time. They publish both the current processing date and the prior service date, so a late GTFS correction for yesterday is replaced by the next nightly run.

GTFS staging spans all loaded snapshots. Downstream models must choose and carry `gtfs_snapshot_id` explicitly.

## Time Semantics

Use these names carefully:

| Field | Meaning |
| --- | --- |
| `gps_date` | Warsaw-local date of raw GPS processing. |
| `service_date` | GTFS service date. Overnight trips can differ from `gps_date`. |
| `processing_date` | Airflow/dbt run date, normally the GPS date being rebuilt. |
| `scheduled_start_date` | Date of scheduled trip start, used by coverage marts. |
| `publish_service_date` | Fact partition being published by the DAG. |

Nightly GPS runs publish current and prior service-date facts. The prior publish lets after-midnight GPS complete previous-service-date trips.

Current service-date facts exclude trips ending after the processed GPS date. Those overnight trips publish on the next run, when the same service date is rebuilt as prior service.

## Core Tables

| Model | Contract |
| --- | --- |
| `stg_gps__pings` | One processing-date slice of cleaned GPS pings; deduped by vehicle and GPS timestamp. |
| `stg_gtfs__*` | Snapshot-aware GTFS staging across all loaded snapshots. |
| `int_gtfs_trip_schedule` | Scheduled trips under the selected snapshot, scoped to processing/service-date overlap. |
| `int_gtfs_duty_chain` | Ordered scheduled duty segments by snapshot, service date, and duty identity. |
| `int_ping_trip` | Settled GPS ping assignment to duty-chain trip candidates. |
| `int_schedule_version` | Timetable-version ranges by `line`, `direction_id`, and `schedule_day_type`. |
| `int_stop_arrivals` | Reconstructed scheduled stop arrivals from GPS movement. |
| `int_trip_summary` | Observed vehicle trip candidates with quality flags. |
| `fct_trip` | Serving fact for observed trips, partitioned by `service_date`. |
| `fct_stop_arrival` | Serving detail fact for detected stop arrivals, partitioned by `service_date`. |
| `fct_expected_stop_event` | Serving trip-detail fact with every scheduled stop for each matched vehicle trip and explicit observation status. |
| `mart_day_completeness` | Raw GPS ingestion coverage by GPS date and mode. |
| `agg_service_coverage` | Schedule-aware observed-service coverage by scheduled start date/hour. |
| `mart_pipeline_status` | Historical archive health by operational date and mode. |

## Dimensions

Archive-safe dimensions are date-ranged where history matters:

- `dim_line`
- `dim_stop_group`
- `dim_stop_post`
- `dim_date`
- `dim_schedule_date`
- `dim_schedule_version`

Current convenience dimensions are present-day lookup surfaces only:

- `dim_line_current`
- `dim_stop_group_current`
- `dim_stop_post_current`
- `dim_schedule_date_current`

`dim_stop_group` groups by the first four characters of `stop_id`. Warsaw bus/tram posts often use six-digit IDs, but the feed also contains metro, rail, depots, entrances, and platforms. Do not assume every stop ID is a six-digit passenger post.

`dim_date` is calendar-only. `dim_schedule_date` is GTFS-service-aware. Use `schedule_day_type` for transit schedule grouping.

## Facts

`fct_trip` grain is one observed vehicle trip candidate. It carries archive-safe mode, route, headsign, origin, destination, schedule-version, and quality fields.

`fct_stop_arrival` grain is one detected scheduled stop arrival per trip candidate and stop sequence. It carries stop labels, line labels, schedule-version lineage, `source_gps_date`, and `delay_seconds`.

`fct_expected_stop_event` grain is one scheduled stop per matched vehicle trip. It attaches direct observations from `fct_stop_arrival` when available and emits `observed`, `missed`, or `uncertain` status for frontend trip timelines. `uncertain` rows can carry raw observation timestamps when the trip assignment is not trustworthy; do not use them as delay evidence.

`delay_seconds = actual_arrival_time - scheduled_arrival_time`. Positive means late. Negative means early.

`hour_bracket` is based on scheduled arrival time in Warsaw local time. Use scheduled hour for leaderboards and distributions so delayed vehicles stay attached to the service they were scheduled to provide.

Quality policy:

- `complete`: default for strict analytics.
- `partial`: acceptable for exploration and drill-down.
- `broken`: debug only.

Quality thresholds are implementation constants, not transport truth. Change them only after real-data validation.

## Schedule Versions

Schedule versions answer: "same line, same direction, same service pattern, same timetable?"

The fingerprint uses ordered scheduled stop/time content. It intentionally excludes snapshot IDs, trip IDs, service IDs, labels, and other display fields, so republishing the same timetable does not create a fake schedule change.

`schedule_version_id` includes `valid_from_date`. If a timetable changes away and later returns, it is a new version period.

The mkuran GTFS feed is a rolling window. Schedule versions are known only from collected snapshots onward. A null `valid_to_date` means no later collected change is known.

## Duty Chains

`int_gtfs_duty_chain` uses GTFS `block_id` as the duty identity. `block_short_name`/`brigade` is only a display and GPS-matching field; it is not unique enough to identify a whole duty chain.

Rows are ordered by scheduled trip time within one `gtfs_snapshot_id`, `service_date`, and duty identity. The model exposes line-change, layover, overlap, negative-duration, and missing-stop diagnostics for matcher work.

When `block_id` is missing, the model falls back to `line:brigade`. Treat fallback rows as weaker lineage.

Depot pull-out and pull-in trips stay in `int_gtfs_duty_chain` for matcher continuity. Use `is_public_service_segment` to exclude depot-only service from public views.

`int_ping_trip` is the settled archive matcher. It assigns each eligible GPS ping to one duty-chain trip candidate using line, timing, duty-chain continuity, and overlap diagnostics.

Spatial and stop-progression scores are reserved for later matcher work. Stop-event reconstruction stays in `int_stop_arrivals`.

## Completeness And Coverage

Do not confuse ingestion completeness with service coverage.

`mart_day_completeness` checks raw GPS arrival by GPS date and mode. It does not know whether scheduled service existed in a missing hour.

`agg_service_coverage` compares expected scheduled bus/tram trips with observed `complete` or `partial` trip candidates. `broken` rows do not count as observed service.

No `agg_service_coverage` row means no scheduled bus/tram service for that slice. A row with `service_coverage_ratio = 0` means scheduled service was not observed.

`mart_pipeline_status` combines historical archive health signals. Near-real-time poller liveness comes from the private heartbeat captured by the serving export, not from this mart.

## Aggregates

Aggregates are serving accelerators over strict-quality stop-arrival detail. They can be rebuilt from facts.

Period aggregates:

- `agg_line_stop_period`
- `agg_stop_period`
- `agg_time_period`

Daily aggregate:

- `agg_line_daily`

Period aggregates support `month` and `schedule_version` periods. Period rows expose `source_start_date`, `source_end_date`, and `is_partial_period`; consumers should label or filter partial windows before comparing them.

Aggregates carry labels and `gtfs_snapshot_ids` from source facts. Do not relabel historical aggregate rows through current dimensions.

## Serving Export

`dag_serving_export` is a manual publication step. It exports a fixed allowlist from `ztm_marts` to GCS Parquet, downloads it in the Airflow worker, builds derived DuckDB serving tables, validates guardrails, and atomically swaps the stable DuckDB file.

The DuckDB artifact is not a mirror of `ztm_marts`. It contains only current frontend source tables, derived serving tables, `export_metadata`, and `export_table_stats`.

Changing the frontend serving surface means changing the export allowlist, derived SQL, tests, and serving contract together.

## Tests

Error tests are for structural invariants:

- uniqueness at declared grain;
- not-null keys;
- enum accepted values;
- relationship checks that protect lineage.

Distribution checks, suspicious delays, low coverage, and quality thresholds should be warnings or model columns unless the data is structurally unusable.

Default Airflow dbt tests stay bounded. Full-history schedule/version tests, broad aggregate tests, broader contract audits, and serving export schema audits are manual jobs.

## Cost Discipline

Large models use `insert_overwrite` with bounded static partitions. Rerunning the same window replaces partitions and should not duplicate rows.

Large partitioned tables must require partition filters. Upstream dbt SQL must filter by the same partition window it overwrites.

Final model projections should list columns explicitly. Avoid `select *` in outputs.

The dbt BigQuery profile sets `maximum_bytes_billed`, defaulting to 100 GB per query. Dry-run large backfills before execution.

Nightly GPS runs log BigQuery dbt job cost metadata from `INFORMATION_SCHEMA.JOBS_BY_USER`. Treat the 100 GiB warning threshold as an operational signal until calibrated.
