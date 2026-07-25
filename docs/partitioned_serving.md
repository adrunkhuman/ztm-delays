# Partitioned Serving Store

## Purpose

BigQuery remains the authoritative historical warehouse. The serving export must preserve every published date without
re-extracting, downloading, and rewriting all prior dates during each nightly publication.

The serving store uses one small DuckDB catalog over locally retained Parquet partitions:

```text
serving/
  ztm.duckdb
  ztm.duckdb.meta.json
  parquet/
    fct_expected_stop_event/service_date=2026-07-24/generation=<export_id>/*.parquet
    mart_trip_daily/service_date=2026-07-24/generation=<export_id>/*.parquet
    mart_mode_window_summary/source_end_date=2026-07-24/generation=<export_id>/*.parquet
    ...
```

The frontend continues to open `ztm.duckdb`. Global reference data is materialized in that file. Date-bearing table names
are DuckDB views over the active local Parquet generations, so existing frontend SQL and metric definitions do not
change.

## Table Boundaries

Global tables are small and replaced with each catalog publication:

- `dim_schedule_version`
- `dim_serving_date`
- `dim_stop_group_current`
- `dim_stop_post_current`
- `mart_pipeline_status_recent_summary`

Every other serving table is partitioned by either `service_date` or `source_end_date`. `service_date` partitions hold
daily facts. `source_end_date` partitions hold aggregates and window membership anchored on one selected date.

The physical partitioning does not alter dbt model logic. Weekday, weekend, and month calculations remain warehouse
outputs; the catalog views expose their existing rows without recomputing metrics in DuckDB.

## Publication

For each changed date:

1. Rebuild the corresponding dbt partitions in BigQuery through the existing daily pipeline.
2. Extract only changed or missing serving partitions to Parquet.
3. Download each new partition under an immutable local generation directory.
4. Validate the candidate catalog against the complete active partition set.
5. Atomically replace the small DuckDB catalog and metadata sidecar.
6. Retain prior local generations long enough for readers holding the previous catalog snapshot to finish.

The catalog is the commit point. A failed extraction, download, build, or validation must leave the previous catalog and
its referenced Parquet generations usable.

Historical corrections use the same path. Every date reported in `changed_partition_dates` receives a new generation;
unrelated partitions are not downloaded or rewritten.

## Growth Model

Keeping every date locally means total VPS storage still grows with history. Partitioning does not compress away required
data. It changes recurring work from O(all retained history) to O(changed partitions):

| Resource | Monolithic export | Partitioned store |
| --- | --- | --- |
| Nightly BigQuery extraction | All serving rows | Changed partitions plus global tables |
| Nightly VPS download | All serving rows | Changed partitions plus global tables |
| Nightly DuckDB rewrite | Complete database | Small catalog only |
| Persistent VPS storage | Grows with history | Grows with history |

Disk capacity remains an operational concern, but adding capacity no longer increases nightly publication time. The
partition store must report total bytes and free-space headroom before publication so capacity can be expanded before it
becomes an outage.

## Migration

The current monolithic exporter remains active until the partitioned path passes the same structural and semantic
validation. Initial migration populates every retained partition once. Subsequent runs are incremental.

Cutover requirements:

- All `MART_TABLES` are represented by either a materialized global table or a partition-backed view.
- Row counts and date ranges match BigQuery metadata.
- Existing frontend tests pass without query-contract changes.
- A failed candidate publication preserves the active catalog and partition generations.
- The runbook covers initial backfill, retries, disk monitoring, and rollback to the monolithic artifact.
