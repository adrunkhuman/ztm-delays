# Warehouse v2

## Layers

The warehouse is split by BigQuery dataset, not by table prefix alone:

| Layer | Dataset | Purpose |
| --- | --- | --- |
| Raw | `ztm_raw` | Rebuildable loads from immutable GCS objects. |
| Staging | `ztm_stg` | Light cleaning, typing, renaming, and structural guards. |
| Intermediate | `ztm_int` | Reusable reconstruction models at GPS/trip/arrival grains. |
| Marts | `ztm_marts` | Frontend/export-facing dimensions, facts, aggregates, and status marts. |

dbt uses `generate_schema_name` to route model layers to exact dataset names. Raw sources use `DBT_BIGQUERY_RAW_DATASET`, defaulting to `ztm_raw`.

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

Historical fact models must be self-contained for archive rendering. `fct_trip` carries line mode and matched-trip headsign from the governing snapshot. Future stop-arrival facts should carry stop name, stop coordinates, stop-group name, line mode, and matched-trip headsign. Use the matched trip's `trip_headsign`, not rolled-up `_current` line headsigns. Later label corrections require rebuilding facts from raw/staging if the archive should reflect the correction.

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

`int_gtfs_trip_schedule` denormalizes scheduled trips under each Warsaw-local GPS processing date's governing GTFS snapshot. The governing snapshot rule is the same rule used by GPS processing: for one processing date, use the latest loaded GTFS snapshot whose Warsaw-local snapshot date is strictly before that processing date. A same-day GTFS snapshot does not govern that same processing date; intraday schedule changes remain out of scope until #27 is implemented. Because GPS matching can use the prior GTFS `service_date` for overnight trips, `int_gtfs_trip_schedule` carries both `processing_date` and `service_date` and keeps only trips whose scheduled window overlaps the processing date. `schedule_day_type` and `schedule_service_ids` on this model are classified per `line` and `direction_id`, so a mixed network service date does not force every line into the `mixed` bucket.

`int_schedule_version` and `dim_schedule_version` implement the "since last schedule change" baseline for line analytics. The grain is one consecutive timetable version for `line`, `direction_id`, and `schedule_day_type`. Public `line` is the working key for now, but schedule-version rows keep governing snapshot lineage so the warehouse can be rebuilt later if a stronger line identity becomes necessary.

The timetable fingerprint is built from the actual scheduled stop/time content only. For each scheduled trip, `trip_timetable_signature` is the ordered list of `(stop_sequence, stop_id, arrival_time_seconds)`. For each `line`, `direction_id`, `schedule_day_type`, and governing processing date, those trip signatures are sorted by absolute scheduled start, absolute scheduled end, and signature, then hashed into `timetable_fingerprint`. The fingerprint deliberately excludes `gtfs_snapshot_id`, `trip_id`, `service_id`, `trip_headsign`, route labels, stop labels, and other display attributes, so rolling-window re-publishes do not create false schedule changes.

`schedule_version_id` changes only when the timetable fingerprint changes between collected governing processing dates for the same `line`, `direction_id`, and `schedule_day_type`. The ID also includes `valid_from_date`, so a timetable that changes away and later returns is represented as a new version period instead of merging across history.

The mkuran GTFS feed is a rolling 31-day window. Schedule versions are therefore only known from collected snapshots onward; versions before collection start are unknowable from warehouse data. `valid_to_date` is null when no later collected timetable change is known, not proof that the public schedule will never change.

`fct_trip` carries `schedule_version_id` from the timetable-version join on `line`, `direction_id`, `schedule_day_type`, and GPS processing date between `valid_from_date` and `coalesce(valid_to_date, date '9999-12-31')`. Future `fct_stop_arrival` should use the same schedule-version lineage. Display labels still come from the governing snapshot and archive-safe label rules, not from schedule-version fingerprinting.

## Completed Trips

`int_trip_summary` is the completed-trip building block for realized service. The grain is one observed vehicle trip candidate per processing `gps_date`, `gtfs_snapshot_id`, `service_date`, `trip_id`, and `vehicle_number`. Each processing run summarizes reconstructed arrivals from the current and previous GPS dates into the current `gps_date` partition so cross-midnight trips can be represented as one trip candidate. The model starts from reconstructed stop arrivals, so v1 includes trips with at least one detected scheduled stop. Matched pings are used for diagnostics such as maximum ping gap and impossible speed jumps, not as standalone ping-only trip facts.

`fct_trip` is the serving-layer projection of that grain, partitioned by `service_date` and clustered by `line` and `direction_id`. Hourly `dag_daily_gps` overwrites the current and previous service-date partitions from the latest available `int_trip_summary` processing partition, so overnight trips can be added without losing prior-day daytime trips. First deploys and manual backfills should build the prior GPS partition before publishing a service date that depends on it. Trip rows carry archive-safe labels from the governing GTFS snapshot: `mode`, `route_short_name`, `trip_headsign`, origin stop, and destination stop. Historical trip pages should render from these baked labels rather than joining `_current` dimensions.

Trip quality is intentionally categorical, not a fake-precise score:

- `complete`: reliable for normal analytics. Stop coverage is high, terminal stops are observed or near-observed, ping gaps are not large, and stop progression is sane.
- `partial`: useful for exploration and drill-down, but missing enough route context that strict analytics should exclude it.
- `broken`: retained for debugging only. These rows show too little stop coverage, implausible progression, impossible GPS movement, extreme delay, or likely wrong trip assignment.

Default filter policy:

- Strict analytics use `trip_quality = 'complete'`.
- Exploratory views may use `trip_quality in ('complete', 'partial')`.
- Debug views may include `broken` and should surface `quality_flags`.

The initial thresholds for stop coverage, terminal-stop tolerance, ping gaps, stop-sequence gaps, impossible speed, and extreme delay are provisional constants in `int_trip_summary`. They are implementation guesses, not settled transport truths. Threshold changes should be backed by real-data validation after enough collected days exist.

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

Large date-partitioned models use `insert_overwrite` with static `partitions` for the processed date. Cost scales with the source partition being rebuilt rather than accumulated table history, and rerunning a date replaces the partition without duplicates.

Large partitioned tables should set `require_partition_filter=true`. dbt models must filter upstream by the same partition they overwrite.

Final model projections must list columns explicitly. Avoid `select *` in outputs because it weakens contracts and can scan unnecessary bytes.

The dbt BigQuery profile sets `maximum_bytes_billed`, defaulting to 100 GB per query. Large backfills should be dry-run before execution.

Intermediate models are materialized as incremental tables when the work is expensive and reused downstream. That is deliberate; the trip/arrival reconstruction should not be recomputed repeatedly as ephemeral SQL.

## Current Cutover

The new datasets are built in parallel with the old `ztm_bq` dataset. `ztm_bq` is confirmed disposable, but it is dropped only after `ztm_raw`, `ztm_stg`, `ztm_int`, and `ztm_marts` are validated.
