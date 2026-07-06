# Warehouse v2

## Layers

The warehouse is split by BigQuery dataset, not by table prefix alone:

| Layer | Dataset | Purpose |
| --- | --- | --- |
| Raw | `ztm_raw` | Rebuildable loads from immutable GCS objects. |
| Staging | `ztm_stg` | Light cleaning, typing, renaming, and structural guards. |
| Intermediate | `ztm_int` | Reusable reconstruction models at GPS/trip/arrival grains. |
| Marts | `ztm_marts` | Frontend/export-facing dimensions, facts, aggregates, and status marts. |

dbt uses `generate_schema_name` to route model layers to exact dataset names. Raw sources use `BIGQUERY_RAW_DATASET`, defaulting to `ztm_raw`.

## Naming

Staging follows dbt's `stg_<source>__<entity>` idiom:

- `stg_gps__pings`
- `stg_gtfs__trips`
- `stg_gtfs__stop_times`
- `stg_gtfs__stops`
- `stg_gtfs__shapes`
- `stg_gtfs__routes`
- `stg_gtfs__calendar_dates`

Intermediate models use `int_<purpose>`. Marts use `dim_`, `fct_`, `agg_`, or `mart_`. Raw source tables keep loader names such as `raw_gps_pings` and `raw_gtfs_trips`.

Archive-safe conformed dimensions:

- `dim_line`
- `dim_stop_group`
- `dim_stop_post`
- `dim_date`
- `dim_schedule_date`
- `dim_schedule_version`

Current convenience lookups:

- `dim_line_current`
- `dim_stop_group_current`
- `dim_stop_post_current`
- `dim_schedule_date_current`

## Staging Contract

GTFS staging spans all loaded snapshots. It does not filter on `gtfs_snapshot_id`; that column is exposed as lineage and downstream models must choose the governing snapshot explicitly.

GPS staging remains date-partitioned because raw GPS is the high-volume input. `stg_gps__pings` processes one Warsaw-local `processing_date`, deduplicates by `vehicle_number` and `gps_time`, normalizes numeric identifiers, and drops structurally impossible coordinates before any geography functions run.

Intermediate models that join schedule data must include `gtfs_snapshot_id` in joins and carry it forward. This prevents historical backfills from silently matching old GPS dates against the newest GTFS snapshot.

## Conformed Dimensions

`dim_line`, `dim_stop_group`, and `dim_stop_post` are archive-safe slowly changing dictionaries. They collapse consecutive governing GTFS snapshots into date-ranged rows and emit a new row only when the display attributes change. Validity dates follow the governing-snapshot rule used by GPS processing: a snapshot first governs the Warsaw-local service date after its snapshot date.

If an entity disappears from a later governing snapshot, the prior visible version closes at the day before the disappearance takes effect. Missing-state rows are not emitted.

`dim_line_current`, `dim_stop_group_current`, `dim_stop_post_current`, and `dim_schedule_date_current` are current-snapshot convenience surfaces rebuilt for the selected `gtfs_snapshot_id`; the normal Airflow path supplies the GTFS snapshot that `dag_gtfs_load` just loaded. Use them for current filters, current maps, and operational/debug views. Do not join historical facts to `_current` dimensions to render archive labels.

Historical fact models must bake display labels from the governing snapshot onto each fact row at build time. The serving/frontend hot path should read those frozen labels directly. `_current` dimensions may be used only when a view intentionally wants present-day labels.

Historical fact models must be self-contained for archive rendering. `fct_trip` carries line mode and matched-trip headsign from the governing snapshot. `fct_stop_arrival` carries stop name, stop coordinates, stop-group name, line mode, and matched-trip headsign. Use the matched trip's `trip_headsign`, not rolled-up `_current` line headsigns. Later label corrections require rebuilding facts from raw/staging if the archive should reflect the correction.

Do not treat public `line` as a stable historical entity by itself. The same public line number can keep its label while schedule patterns, directions, or stop sets change. Historical facts and aggregates must join through GTFS snapshot or schedule-version lineage before comparing a line across time.

### Modes

GTFS `route_type` maps to the warehouse `mode` column as follows:

| `route_type` | `mode` |
| --- | --- |
| `0` | `tram` |
| `1` | `metro` |
| `2` | `rail` |
| `3` | `bus` |

Only `bus` and `tram` have live GPS pings from the current poller. `metro` and `rail` are schedule-only until those sources are explicitly added.

### Stop Hierarchy

GTFS `stop_id` identifies a physical stop post or related transit location. Warsaw bus/tram passenger posts usually use a six-digit shape where the first four digits identify the user-facing stop group and the last two identify the physical post. The full multimodal feed also contains metro, rail, depot, entrance, and platform records with other ID shapes, so the durable grouping rule is the first four characters, not universal six-digit IDs:

```text
stop_group_id = LEFT(stop_id, 4)
stop_id       = stop_group_id || location_suffix
```

`dim_stop_post` is the date-ranged stop/location dictionary keyed by `stop_id` plus validity range. `dim_stop_group` is the date-ranged user-facing parent keyed by `stop_group_id` plus validity range. Current map views should use the `_current` variants.

Most groups share one stop name across posts, but large interchange groups can contain multiple names under the same parent group. `dim_stop_group.stop_group_name` is therefore a deterministic display name chosen from the most common post name, while `stop_group_names` retains all distinct names in the group.

### Date Classification

`dim_date` exposes immutable calendar concepts:

- `day_type`: calendar weekday/weekend classification from `service_date` only.
- `is_holiday`: Polish public-holiday flag from fixed-date holidays and Easter-based movable holidays.

`dim_schedule_date` exposes `schedule_day_type`, the service pattern that actually ran according to GTFS `service_id` assignments in the governing snapshot for each governable `service_date`. Governable dates have a loaded snapshot whose Warsaw-local snapshot date is strictly before `service_date`. If that governing snapshot has no active service IDs for the date, `dim_schedule_date` emits `schedule_day_type = 'unknown'` and empty service-id lineage rather than falling back to an older snapshot. A calendar weekday can still have `schedule_day_type = 'sunday_holiday'` when GTFS says holiday/Sunday service ran. Current GTFS service IDs use provider tokens such as `Pc`, `Pt`, `Sb`, and `Nd`, and surface schedules can prefix those tokens with a pattern date such as `2026-07-01:PcS`. Date-prefixed service IDs preserve specific weekday patterns, so a later date reusing `2026-07-01:PcS` classifies as `wednesday`, not generic `weekday`. Generic service IDs, currently used by metro, remain coarser and map to `weekday`, `friday`, `saturday`, or `sunday_holiday`. `schedule_day_types` and `schedule_service_ids` retain the raw derivation lineage.

Intermediate GPS/trip models currently carry `day_type` from GTFS staging, which is only calendar weekday/weekend. Do not use intermediate `day_type` as a transit schedule-pattern field; use `dim_schedule_date` or baked fact columns when a mart needs `schedule_day_type`.

`route_long_name` is currently exposed as null in line dimensions because the raw GTFS route loader does not retain that optional field.

## Schedule Versions

`int_gtfs_trip_schedule` denormalizes scheduled trips under each GTFS service date's governing snapshot for every Warsaw-local GPS processing date whose raw pings can overlap that service. The governing snapshot rule is service-date based: use the latest loaded GTFS snapshot whose Warsaw-local snapshot date is strictly before `service_date`. A same-day GTFS snapshot does not govern that same service date; intraday schedule changes remain out of scope until #27 is implemented. Because GPS matching can use the prior GTFS `service_date` for overnight trips, `int_gtfs_trip_schedule` carries both `processing_date` and `service_date` and keeps only trips whose scheduled window overlaps the processing date. `schedule_day_type` and `schedule_service_ids` on this model are classified per `line` and `direction_id`, so a mixed network service date does not force every line into the `mixed` bucket.

`int_schedule_version` and `dim_schedule_version` implement the "since last schedule change" baseline for line analytics. The grain is one consecutive timetable version for `line`, `direction_id`, and `schedule_day_type`. Public `line` is the working key for now, but schedule-version rows keep governing snapshot lineage so the warehouse can be rebuilt later if a stronger line identity becomes necessary.

The timetable fingerprint is built from the actual scheduled stop/time content only. For each scheduled trip, `trip_timetable_signature` is the ordered list of `(stop_sequence, stop_id, arrival_time_seconds)`. For each `line`, `direction_id`, `schedule_day_type`, and governing processing date, those trip signatures are sorted by absolute scheduled start, absolute scheduled end, and signature, then hashed into `timetable_fingerprint`. The fingerprint deliberately excludes `gtfs_snapshot_id`, `trip_id`, `service_id`, `trip_headsign`, route labels, stop labels, and other display attributes, so rolling-window re-publishes do not create false schedule changes.

`schedule_version_id` changes only when the timetable fingerprint changes between collected governing processing dates for the same `line`, `direction_id`, and `schedule_day_type`. The ID also includes `valid_from_date`, so a timetable that changes away and later returns is represented as a new version period instead of merging across history.

The mkuran GTFS feed is a rolling 31-day window. Schedule versions are therefore only known from collected snapshots onward; versions before collection start are unknowable from warehouse data. `valid_to_date` is null when no later collected timetable change is known, not proof that the public schedule will never change.

`fct_trip` carries `schedule_version_id` from the timetable-version join on `line`, `direction_id`, `schedule_day_type`, and GPS processing date between `valid_from_date` and `coalesce(valid_to_date, date '9999-12-31')`. `fct_stop_arrival` uses the same schedule-version lineage. Display labels still come from the governing snapshot and archive-safe label rules, not from schedule-version fingerprinting.

## Completed Trips

`int_trip_summary` is the completed-trip building block for realized service. The grain is one observed vehicle trip candidate per processing `gps_date`, `gtfs_snapshot_id`, `service_date`, `trip_id`, and `vehicle_number`. Each processing run summarizes reconstructed arrivals from the current and previous GPS dates into the current `gps_date` partition so cross-midnight trips can be represented as one trip candidate. The model starts from reconstructed stop arrivals, so v1 includes trips with at least one detected scheduled stop. Matched pings are used for diagnostics such as maximum ping gap and impossible speed jumps, not as standalone ping-only trip facts.

`fct_trip` is the serving-layer projection of that grain, partitioned by `service_date`, requiring a partition filter, and clustered by `line` and `direction_id`. The model publishes `publish_service_date`, defaulting to the GPS processing date. Production Airflow rebuilds both the current and prior service-date partitions for each GPS date so after-midnight GPS can complete overnight trips without deleting daytime rows from the prior day. Trip rows carry archive-safe labels from the governing GTFS snapshot: `mode`, `route_short_name`, `trip_headsign`, origin stop, and destination stop. Historical trip pages should render from these baked labels rather than joining `_current` dimensions.

Trip quality is intentionally categorical, not a fake-precise score:

- `complete`: reliable for normal analytics. Stop coverage is high, terminal stops are observed or near-observed, ping gaps are not large, and stop progression is sane.
- `partial`: useful for exploration and drill-down, but missing enough route context that strict analytics should exclude it.
- `broken`: retained for debugging only. These rows show too little stop coverage, implausible progression, impossible GPS movement, extreme delay, or likely wrong trip assignment.

Default filter policy:

- Strict analytics use `trip_quality = 'complete'`.
- Exploratory views may use `trip_quality in ('complete', 'partial')`.
- Debug views may include `broken` and should surface `quality_flags`.

The initial thresholds for stop coverage, terminal-stop tolerance, ping gaps, stop-sequence gaps, impossible speed, and extreme delay are provisional constants in `int_trip_summary`. They are implementation guesses, not settled transport truths. Threshold changes should be backed by real-data validation after enough collected days exist.

## Stop-Arrival Detail

`fct_stop_arrival` is the primary analytical detail grain for drill-downs, leaderboards, and beeswarm distributions. The grain is one detected scheduled stop arrival per `gtfs_snapshot_id`, `service_date`, `trip_id`, `vehicle_number`, and `stop_sequence`. It is built from `int_stop_arrivals` and inherits completed-trip lineage from `fct_trip`, including publishing `gps_date`, `schedule_version_id`, `trip_quality`, `quality_flags`, `schedule_day_type`, `mode`, and matched-trip `trip_headsign`. `source_gps_date` is the GPS partition that produced the underlying stop-arrival reconstruction.

The fact carries archive-safe labels directly on each row: stop name, stop coordinates, stop-group name, route short name, mode, and trip headsign all come from the governing GTFS snapshot used for matching. Historical stop drill-downs, hour-bracket leaderboards, and beeswarm points should render from those baked labels rather than joining `_current` dimensions. `_current` stop and line dimensions remain present-day convenience surfaces only.

`delay_seconds` is `actual_arrival_time - scheduled_arrival_time` in seconds. Positive values mean late arrivals; negative values mean early arrivals. `hour_bracket` is the Warsaw-local hour floor of `scheduled_arrival_time`, so scheduled arrivals from `08:00:00` through `08:59:59` share the `08:00` bracket. Leaderboards and time-of-day distributions should use the scheduled hour bracket, not the actual-arrival hour, so delayed vehicles stay attached to the service they were scheduled to provide.

The model is partitioned by `service_date`, requires a partition filter, and is clustered by `line`, `stop_group_id`, and `hour_bracket`. This matches the expected serving filters for line pages, stop pages, and hour-bracket leaderboards while keeping monthly detail scans bounded. Expected volume is roughly 300-400k rows/day, around 10M rows/month before strict-quality filtering.

Strict analytics default to `trip_quality = 'complete'`. Exploratory views may include `partial`; debug views may include `broken` and should expose `quality_flags`. The detail rows remain available even when a day is later marked incomplete by coverage/completeness marts.

## Completeness And Service Coverage

Trip facts are the analytics validity grain. A day can be incomplete while individual `complete` trips from that day remain valid for strict analytics. Completeness and coverage marts exist so downstream consumers can warn, filter, or annotate partial operational windows instead of dropping whole days blindly.

`mart_day_completeness` summarizes raw GPS ingestion by `gps_date` and mode. It answers: did GPS data arrive for bus/tram during each expected Warsaw-local hour? `expected_hours`, `present_hours`, `missing_hours`, `completeness_ratio`, and `is_complete_day` are operational coverage signals. They do not say whether scheduled service existed in those hours.

`agg_service_coverage` summarizes schedule-aware service coverage by line, direction, headsign, scheduled-start date, and scheduled hour for live-GPS modes only: bus and tram. Expected trips come from `int_gtfs_trip_schedule` under the governing snapshot. Observed trips come from `int_trip_summary` and include distinct `trip_id` values with `complete` or `partial` quality; `broken` rows are excluded so likely wrong trip assignments do not inflate coverage. The intermediate source is used here because it retains previous-service-date after-midnight trips in the processing-date window. If duplicate vehicle candidates exist for the same scheduled trip, the best available quality wins. Trips are counted against their scheduled start hour, not the live GPS hour when they happened to be observed.

`agg_service_coverage` emits rows only for scheduled bus/tram service hours. A zero `service_coverage_ratio` means scheduled service was not observed; no scheduled service is represented by absence of a row. `is_settled_hour` is true only after `service_hour_end < current_timestamp() - 90 minutes`. Frontend/live views should avoid treating unsettled low `service_coverage_ratio` as data loss because delayed trips can still arrive in the model after their scheduled hour. Full-day trend views should use `mart_day_completeness.is_complete_day` and `agg_service_coverage.service_coverage_ratio` together: raw completeness explains ingestion gaps, while service coverage separates scheduled-but-unobserved service from hours with no scheduled row.

`mart_pipeline_status` is the historical archive-health surface by Warsaw-local status date and mode. Its `service_date` column is aligned to the GPS processing date and scheduled-start date, so ingestion, matching, trip quality, arrivals, and service coverage use one operational-day grain; it is not necessarily the GTFS service_date for overnight trips. The manual DuckDB export does not update `last_export_at`; serving freshness comes from DuckDB `export_metadata` until a status-watermark update is added. Near-real-time poller liveness is intentionally separate from this mart and comes from the private poller heartbeat.

## Serving Export

`dag_serving_export` is a manual alpha export that publishes one frontend-serving DuckDB file. It exports only the BigQuery marts needed directly by the current frontend or by DuckDB-derived page tables, stages those source tables as GCS Parquet, downloads them in the Airflow worker, builds derived serving tables locally, validates row/table guardrails, then atomically swaps the stable file path. Changing the frontend serving surface requires updating the DAG source allowlist, derived-table SQL, tests, and serving contract together.

The export is intentionally serving-only. It does not change mart semantics, does not implement the future settled nightly matcher, and does not remove the current hourly BigQuery/modeling path. Its `export_metadata.source_mode` is `current_pipeline_provisional` until the backend is redesigned around settled nightly archive processing.

The serving artifact is not a generic mirror of `ztm_marts`. It includes the current frontend source tables, derived DuckDB aggregate/event tables, `export_metadata`, and `export_table_stats`. Current-snapshot `_current` tables are exported only where the frontend needs present-day stop lookup surfaces; archive views should still render labels from label-bearing facts and aggregates. The frontend should reopen DuckDB connections when `export_metadata.export_id` changes instead of restarting the container.

## Aggregate Marts

The aggregate marts are pure serving accelerators over `fct_stop_arrival`. They use only `trip_quality = 'complete'` rows and can be rebuilt from detail without data loss.

Aggregate period types:

- `month`: calendar month, with `period_id = YYYY-MM` and `period_start_date` set to the first day of the month. Month rows only include source rows from the latest `schedule_version_id` observed in that month for each `line`, `direction_id`, and `schedule_day_type`; "latest" is ordered by `dim_schedule_version.valid_from_date`, not by lexical ID. Earlier in-month schedule versions remain available through `schedule_version` period rows.
- `schedule_version`: per-line timetable-version period, with `period_id = schedule_version_id` and date bounds from `dim_schedule_version`.

Period aggregates expose `source_start_date`, `source_end_date`, and `is_partial_period`. `is_partial_period` is true when the built source window does not cover the natural period bounds, such as a current month before month end or a bounded backfill that starts after a schedule version began. Open-ended `schedule_version` rows have null `period_end_date`, so `is_partial_period` does not mean "still accumulating" for the open side; use `source_end_date` as the observed-through date. Consumers should label or exclude partial periods when comparing complete periods.

Aggregate day-class rows:

- `day_type`: `weekday` or `weekend`.
- `weekday`: lowercase weekday name, Monday through Sunday.
- `schedule_day_type`: GTFS-derived service pattern. Holiday service grouping is represented here because Warsaw holiday schedules usually follow a GTFS service pattern such as Sunday/holiday rather than a pure calendar label.

Shared delay statistics:

- `n`: strict-quality stop-arrival row count in the cell.
- `mean_delay_seconds`: average `delay_seconds`.
- `p10_delay_seconds`, `median_delay_seconds`, `p50_delay_seconds`, `p90_delay_seconds`: approximate BigQuery quantiles over `delay_seconds`.
- `stddev_delay_seconds`: sample standard deviation, null for one-row cells.
- `on_time_rate`: share of rows with `delay_seconds` between `-60` and `180` inclusive.

Fixed histogram buckets are stored as an array of structs with `bucket_label`, `min_delay_seconds`, `max_delay_seconds`, and `n`. Open-ended buckets use null for the unbounded side:

- `early_over_5m`: `< -300` seconds.
- `early_1_to_5m`: `-300..-61` seconds.
- `on_time`: `-60..180` seconds.
- `late_3_to_5m`: `181..300` seconds.
- `late_5_to_10m`: `301..600` seconds.
- `late_10_to_20m`: `601..1200` seconds.
- `late_over_20m`: `> 1200` seconds.

Aggregate model roles:

- `agg_line_stop_period`: line axis, by line, direction, archive-safe headsign, stop post, scheduled Warsaw-local hour, period, and day class.
- `agg_stop_period`: stop axis, by stop group, line, direction, archive-safe headsign, scheduled Warsaw-local hour, period, and day class. Direction remains explicit because schedule versions are per line and direction.
- `agg_time_period`: time-of-day axis, by scheduled Warsaw-local hour, day class, period, and mode. Month rows are network-wide within mode; schedule-version rows are line/direction/headsign-scoped because `schedule_version_id` is per line.
- `agg_line_daily`: daily line trend surface, by line, direction, archive-safe headsign, service date, and schedule version. Unlike month-period rows, daily rows retain every observed `schedule_version_id` because there is no separate daily schedule-version fallback.

The aggregate marts carry display labels from the label-bearing source facts and preserve `gtfs_snapshot_ids` lineage. Frontend code must not relabel historical aggregate rows through `_current` dimensions.

DuckDB latency over the real `2026-06-30` detail partition showed these aggregates are serving/cache conveniences at current volume, not a hard feasibility requirement.

## Error Policy

Structural garbage is removed at staging when it cannot be analyzed safely, for example non-numeric vehicle identifiers or coordinates outside the Warsaw bounding box. Later model layers should flag suspicious but analyzable behavior with quality columns instead of failing an entire run.

A single bad vehicle should become a flagged or broken trip in later issues. It should not fail the whole warehouse build.

## Test Severity

`error`-severity tests are reserved for structural invariants:

- uniqueness at the declared grain;
- not-null keys;
- enum `accepted_values` checks.

Distributional checks, low coverage, suspicious delays, and quality thresholds should be warnings or model columns unless the data is structurally unusable.

## BigQuery Cost Rules

Large date-partitioned models use `insert_overwrite` with bounded static `partitions` for the affected date or period-start partitions. Cost scales with the source partitions being rebuilt rather than accumulated table history, and rerunning the same window replaces partitions without duplicates.

Large partitioned tables should set `require_partition_filter=true`. dbt models must filter upstream by the same partition they overwrite.

Final model projections must list columns explicitly. Avoid `select *` in outputs because it weakens contracts and can scan unnecessary bytes.

The dbt BigQuery profile sets `maximum_bytes_billed`, defaulting to 100 GB per query. Large backfills should be dry-run before execution.

Intermediate models are materialized as incremental tables when the work is expensive and reused downstream. That is deliberate; the trip/arrival reconstruction should not be recomputed repeatedly as ephemeral SQL.

Airflow cadence is cost-gated: hourly raw GPS loading does not trigger the warehouse graph. `dag_daily_gps` runs at `04:00 Europe/Warsaw` by default and can still be manually triggered for one `processing_date`.

dbt tests are tiered. Default Airflow paths exclude known expensive schedule/version tests and broad aggregate mart tests for `agg_line_stop_period`, `agg_stop_period`, `agg_time_period`, and `agg_line_daily` while keeping test selection explicit. Full-history schedule/version tests on `int_gtfs_trip_schedule` and `int_schedule_version`, aggregate mart contract tests for those broad serving aggregates, broader contract audits, and serving export schema audits are explicit manual jobs until weekly/manual audit operations are mature.

Schedule/version manual audits use singular contract tests for required fields and accepted values instead of repeated generic column tests over the expensive views.

Nightly Airflow still tests `mart_day_completeness`, `agg_service_coverage`, and `mart_pipeline_status`; only the four broad serving aggregate tests moved to manual audits.

`mart_day_completeness`, `agg_service_coverage`, `agg_line_daily`, and `mart_pipeline_status` are incremental partition replacements over the inclusive `[aggregation_start_date, processing_date]` date window; normal Airflow runs use a two-day prior/current window because complete/partial observed trips can lag scheduled-start date by one GPS date and facts publish both current and prior service dates. A 2026-06-25-through-current check found no trip-summary lag beyond one day. The period marts `agg_line_stop_period`, `agg_stop_period`, and `agg_time_period` are incremental replacements for affected month-start and schedule-version-start `period_start_date` partitions. Airflow computes `period_source_start_date` from affected month starts and active schedule-version starts so rows include their required source range without rebuilding unrelated retained history.

Nightly GPS runs log BigQuery dbt job cost metadata from `INFORMATION_SCHEMA.JOBS_BY_USER` after the dbt phases complete. The initial warning threshold is `100 GiB` billed bytes for the DAG-run window; treat it as an operational signal until it is calibrated from observed good runs. Attribution is best-effort because the query is scoped to the same BigQuery principal, project, and region, and filters on dbt query comments.

## Current Cutover

The new datasets are built in parallel with the old `ztm_bq` dataset. `ztm_bq` is confirmed disposable, but it is dropped only after `ztm_raw`, `ztm_stg`, `ztm_int`, and `ztm_marts` are validated.
