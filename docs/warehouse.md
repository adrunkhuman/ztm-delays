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

Current conformed dimensions:

- `dim_line`
- `dim_stop_group`
- `dim_stop_post`
- `dim_date`

## Staging Contract

GTFS staging spans all loaded snapshots. It does not filter on `gtfs_snapshot_id`; that column is exposed as lineage and downstream models must choose the governing snapshot explicitly.

GPS staging remains date-partitioned because raw GPS is the high-volume input. `stg_gps__pings` processes one Warsaw-local `processing_date`, deduplicates by `vehicle_number` and `gps_time`, normalizes numeric identifiers, and drops structurally impossible coordinates before any geography functions run.

Intermediate models that join schedule data must include `gtfs_snapshot_id` in joins and carry it forward. This prevents historical backfills from silently matching old GPS dates against the newest GTFS snapshot.

## Conformed Dimensions

`dim_line`, `dim_stop_group`, `dim_stop_post`, and `dim_date` are current-snapshot dimensions. They are rebuilt for the selected `gtfs_snapshot_id`; the normal Airflow path supplies the GTFS snapshot that `dag_gtfs_load` just loaded. This is deliberate for the first serving contract: the frontend needs current lookup tables, while historical schedule-version analysis lands in later models.

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

`dim_stop_post` is the stop/location dimension keyed by `stop_id`. `dim_stop_group` is the user-facing parent keyed by `stop_group_id`.

Most groups share one stop name across posts, but large interchange groups can contain multiple names under the same parent group. `dim_stop_group.stop_group_name` is therefore a deterministic display name chosen from the most common post name, while `stop_group_names` retains all distinct names in the group.

### Date Classification

`dim_date` exposes three related but intentionally separate concepts:

- `day_type`: calendar weekday/weekend classification from `service_date` only.
- `is_holiday`: Polish public-holiday flag from fixed-date holidays and Easter-based movable holidays.
- `schedule_day_type`: service pattern that actually ran according to GTFS `service_id` assignments.

`schedule_day_type` is authoritative for transit pattern grouping within the selected/current GTFS snapshot represented by `dim_date.gtfs_snapshot_id`. A calendar weekday can still have `schedule_day_type = 'sunday_holiday'` when GTFS says holiday/Sunday service ran. Current GTFS service IDs use provider tokens such as `Pc`, `Pt`, `Sb`, and `Nd`, and surface schedules can prefix those tokens with a pattern date such as `2026-07-01:PcS`. Date-prefixed service IDs preserve specific weekday patterns, so a later date reusing `2026-07-01:PcS` classifies as `wednesday`, not generic `weekday`. Generic service IDs, currently used by metro, remain coarser and map to `weekday`, `friday`, `saturday`, or `sunday_holiday`. `schedule_day_types` and `schedule_service_ids` retain the raw derivation lineage.

Intermediate GPS/trip models currently carry `day_type` from GTFS staging, which is only calendar weekday/weekend. Do not use intermediate `day_type` as a transit schedule-pattern field; join to `dim_date` by `service_date` and the selected snapshot when a mart needs `schedule_day_type` or `is_holiday`.

`route_long_name` is currently exposed as null in `dim_line` because the raw GTFS route loader does not retain that optional field.

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
