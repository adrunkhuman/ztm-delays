# Serving Contract

The frontend should read historical labels from facts and aggregate marts by default. Do not join historical rows to `_current` dimensions for display labels.

Fallback rule: every chart backed by `agg_line_stop_period`, `agg_stop_period`, `agg_time_period`, or `agg_line_daily` must be able to render from `fct_stop_arrival` when an aggregate cell is absent or stale. For now, stale means the aggregate's `source_end_date` is older than the requested detail range or `is_partial_period` is not acceptable for the comparison being shown. A later pipeline-status mart can provide a stronger build watermark. Aggregates are serving accelerators, not the source of truth.

Strict public analytics default to `trip_quality = 'complete'`. Exploratory views may include `partial`; debug views may include `broken` and surface `quality_flags`.
