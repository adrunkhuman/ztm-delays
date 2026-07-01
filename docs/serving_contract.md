# Serving Contract

The frontend should read historical labels from facts and aggregate marts by default. Do not join historical rows to `_current` dimensions for display labels.

Fallback rule: every chart backed by `agg_line_stop_period`, `agg_stop_period`, `agg_time_period`, or `agg_line_daily` must be able to render from `fct_stop_arrival` when an aggregate cell is absent or stale. For now, stale means the aggregate's `source_end_date` is older than the requested detail range or `is_partial_period` is not acceptable for the comparison being shown. `mart_pipeline_status` provides day/mode archive-health context but does not replace detail fallback checks. Aggregates are serving accelerators, not the source of truth.

Strict public analytics default to `trip_quality = 'complete'`. Exploratory views may include `partial`; debug views may include `broken` and surface `quality_flags`.

## Status Panel

Historical/archive health comes from `mart_pipeline_status`, keyed by `service_date` and `mode`. The frontend should use it for ingestion completeness, GPS match rate, trip quality counts, settled service coverage, stop-arrival output counts, and GTFS snapshot freshness. `last_export_at` is null until the serving export job is implemented.

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
