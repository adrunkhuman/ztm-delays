# Pipeline Runbook

## From-Scratch Rebuild

Raw BigQuery tables are rebuildable from immutable GCS inputs:

- GPS: `gs://ztm-analytics-bucket/raw/gps/**/part-*.parquet`
- GTFS: `gs://ztm-analytics-bucket/raw/gtfs/*.zip`

Rebuild order:

1. Recreate or empty the target v2 datasets: `ztm_raw`, `ztm_stg`, `ztm_int`, `ztm_marts`.
2. Rebuild `raw_gtfs_snapshots` deterministically from GCS object names and file hashes. Object names encode `snapshot_timestamp`; `snapshot_id` is `{snapshot_timestamp}_{sha256[:12]}`.
3. Reload each GTFS ZIP into `ztm_raw.raw_gtfs_*` with deterministic load job IDs.
4. Run GTFS staging, then rebuild archive-safe dimensions: `dim_line`, `dim_stop_post`, `dim_stop_group`, `dim_date`, and `dim_schedule_date`.
5. Rebuild schedule-version models from loaded GTFS history: `int_gtfs_trip_schedule`, `int_schedule_version`, and `dim_schedule_version`.
6. Build `_current` lookup tables only for the selected serving snapshot.
7. Reload GPS Parquet files into `ztm_raw.raw_gps_pings` with deterministic per-URI load job IDs.
8. Build dbt models by processing date and governing GTFS snapshot.
9. Validate backfilled facts before retiring `ztm_bq`.

Do not drop `ztm_bq` before the v2 datasets are validated.

## Per-Date Rebuild

For one GPS date, load raw GPS parts first, then run dbt with the Warsaw-local processing date and governing GTFS snapshot:

```bash
dbt build --select stg_gps__pings int_gps_hourly_completeness \
  --vars '{"processing_date":"YYYY-MM-DD"}'

dbt build --select stg_gtfs__trips stg_gtfs__stop_times stg_gtfs__calendar_dates int_ping_trip \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'

dbt build --select stg_gtfs__stop_times stg_gtfs__stops int_stop_arrivals \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'

dbt build --select int_trip_summary fct_trip fct_stop_arrival \
  --vars '{"processing_date":"YYYY-MM-DD","gtfs_snapshot_id":"SNAPSHOT_ID"}'
```

GPS staging and intermediate models use static-partition `insert_overwrite` for the selected `processing_date`. Serving facts are partitioned by `service_date` and currently overwrite only the selected service date. Prior-service-date completion from after-midnight GPS is deferred until the pipeline can rebuild the full prior-day service partition without deleting daytime rows.

## Date-Range Backfill

Loop over dates in order. For every date, resolve the governing snapshot from `ztm_raw.raw_gtfs_snapshots`: latest snapshot whose Warsaw-local `snapshot_timestamp` date is before the GPS `processing_date`.

After backfill, verify that facts carry the expected `gtfs_snapshot_id` for each `service_date`. Also verify historical facts carry baked line/stop/schedule labels from the governing snapshot and do not join `_current` dimensions for archive rendering. A successful run is not enough; matching every historical date against the newest snapshot is silent corruption. Stop-arrival facts carry both publishing `gps_date` and `source_gps_date`; use `source_gps_date` when debugging which raw GPS partition produced an individual stop detection.

## Operational Notes

- Raw load retries are safe because job IDs are deterministic.
- One malformed GPS coordinate should be filtered at staging and must not fail stop-arrival reconstruction.
- Large ad-hoc BigQuery work should be dry-run and bounded by `maximum_bytes_billed`.
- Keep old `ztm_bq` objects until v2 validation passes.
