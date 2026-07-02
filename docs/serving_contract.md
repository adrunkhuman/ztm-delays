# Serving Contract

The alpha frontend reads a local DuckDB file produced manually by `dag_serving_export`:

```text
ztm.duckdb
```

The export includes the fixed `MART_TABLES` allowlist, currently intended to mirror all current serving marts in `ztm_marts`, plus `export_metadata` and `export_table_stats`. When a new serving mart is added, update the Airflow allowlist, export tests, and this contract together. The file is built to a temporary path and atomically swapped into the stable path, so frontend containers do not need to restart after a rebuild. Frontend code should avoid one permanent DuckDB connection; open per request or reopen cached connections when `export_metadata.export_id` changes.

If the frontend watches the serving directory, it should react only to `ztm.duckdb` and `ztm.duckdb.meta.json`. Ignore hidden export build artifacts such as `.duckdb-tmp-*`, `.*.tmp`, and `*.wal`.

Alpha export source mode is `current_pipeline_provisional`: the serving file reflects the current marts as built by the existing pipeline. It is not yet the later settled nightly matcher/export design.

The frontend should read historical labels from facts and aggregate marts by default. Do not join historical rows to `_current` dimensions for display labels.

Exported mart tables:

- `dim_line`
- `dim_line_current`
- `dim_stop_post`
- `dim_stop_post_current`
- `dim_stop_group`
- `dim_stop_group_current`
- `dim_date`
- `dim_schedule_date`
- `dim_schedule_date_current`
- `dim_schedule_version`
- `fct_trip`
- `fct_stop_arrival`
- `mart_day_completeness`
- `agg_service_coverage`
- `mart_pipeline_status`
- `agg_line_stop_period`
- `agg_stop_period`
- `agg_time_period`
- `agg_line_daily`

Export metadata tables:

- `export_metadata`: one row with `export_id`, `export_version`, `source_mode`, `exported_at`, source dataset identifiers, source row/byte totals, exported table count, and DuckDB file size.
- `export_table_stats`: one row per exported mart with source row count, source bytes, and date range where the mart has a primary date field.

The frontend should display `export_metadata.exported_at` and the available service-date range from `export_table_stats` so alpha users can see the archive freshness explicitly. The optional sidecar JSON is named `ztm.duckdb.meta.json` by default and mirrors the export summary for operations. It is written after the DuckDB swap, so the database metadata is the source of truth if the two briefly disagree.

Fallback rule: every chart backed by `agg_line_stop_period`, `agg_stop_period`, `agg_time_period`, or `agg_line_daily` must be able to render from `fct_stop_arrival` when an aggregate cell is absent or stale. For now, stale means the aggregate's `source_end_date` is older than the requested detail range or `is_partial_period` is not acceptable for the comparison being shown. `mart_pipeline_status` provides day/mode archive-health context but does not replace detail fallback checks. Aggregates are serving accelerators, not the source of truth.

Strict public analytics default to `trip_quality = 'complete'`. Exploratory views may include `partial`; debug views may include `broken` and surface `quality_flags`.

## Status Panel

Historical/archive health comes from `mart_pipeline_status`, keyed by `service_date` and `mode`. The frontend should use it for ingestion completeness, GPS match rate, trip quality counts, settled service coverage, stop-arrival output counts, and GTFS snapshot freshness. The manual DuckDB export does not update `mart_pipeline_status.last_export_at`; use `export_metadata.exported_at` or the sidecar JSON for export freshness until a status-watermark update is added.

Near-real-time poller liveness comes from the private GCS heartbeat object written by the poller:

```text
gs://ztm-analytics-bucket/health/poller/latest.json
```

The browser should not read that object directly. The serving/export layer should read it with backend credentials and expose a sanitized `poller_status` object to the frontend. Treat the poller as stale when `updated_at` is older than 180 seconds. Use the heartbeat `status` field directly when the object is fresh: `ok`, `degraded`, `down`, or `starting`.

Frontend shape:

```json
{
  "status": "ok | degraded | down | starting | stale",
  "updated_at": "2026-01-15T12:00:00Z",
  "is_stale": false,
  "stale_after_seconds": 180,
  "modes": {
    "bus": {
      "last_success_at": "2026-01-15T11:59:50Z",
      "consecutive_failures": 0,
      "last_error_type": null
    },
    "tram": {
      "last_success_at": "2026-01-15T11:59:50Z",
      "consecutive_failures": 0,
      "last_error_type": null
    }
  }
}
```

If the heartbeat is stale, expose `status = "stale"` and `is_stale = true` regardless of the raw heartbeat status.
