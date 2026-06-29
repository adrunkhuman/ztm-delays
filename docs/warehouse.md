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

## Staging Contract

GTFS staging spans all loaded snapshots. It does not filter on `gtfs_snapshot_id`; that column is exposed as lineage and downstream models must choose the governing snapshot explicitly.

GPS staging remains date-partitioned because raw GPS is the high-volume input. `stg_gps__pings` processes one Warsaw-local `processing_date`, deduplicates by `vehicle_number` and `gps_time`, normalizes numeric identifiers, and drops structurally impossible coordinates before any geography functions run.

Intermediate models that join schedule data must include `gtfs_snapshot_id` in joins and carry it forward. This prevents historical backfills from silently matching old GPS dates against the newest GTFS snapshot.

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
