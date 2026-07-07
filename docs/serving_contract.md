# Serving Contract

The alpha frontend reads one local DuckDB file:

```text
ztm.duckdb
```

The file is a frontend-serving artifact, not a generic mirror of every warehouse mart. `dag_serving_export` builds the file from a small source-table allowlist, derives page-shaped aggregate tables inside DuckDB, validates the result, then atomically swaps the stable file path. The frontend mounts the serving directory read-only and opens DuckDB connections per query, so daily/manual file replacement does not require a frontend container restart.

Current alpha source mode is `current_pipeline_provisional`. These facts come from the current matcher pipeline and are not the future settled nightly archive. The frontend should surface this as an alpha/provisional archive when explanatory copy is needed.

## Runtime Files

The serving directory contains:

```text
ztm.duckdb
ztm.duckdb.meta.json
```

The database is the source of truth. The JSON sidecar mirrors export metadata for operations and may briefly lag during file swaps. Frontend file watchers, if added later, should react only to `ztm.duckdb` and `ztm.duckdb.meta.json`; ignore hidden build artifacts such as `.duckdb-tmp-*`, `.*.tmp`, `*.tmp`, and `*.wal`.

Current VPS path convention:

```text
host:      /home/ubuntu/ztm-pipeline/serving
frontend:  /app/serving
env:       ZTM_DUCKDB_PATH=/app/serving/ztm.duckdb
```

## Source Tables

`dag_serving_export` exports only the BigQuery marts needed directly by the current frontend or by derived DuckDB tables:

- `agg_line_daily`
- `dim_stop_group_current`
- `dim_stop_post_current`
- `fct_expected_stop_event`
- `fct_stop_arrival`
- `fct_trip`
- `mart_pipeline_status`

These tables should keep archive-safe display labels on facts/aggregates. Historical frontend views must not relabel facts by joining to `_current` dimensions unless the view is explicitly present-day/current-state oriented.

## Derived Tables

The export derives these DuckDB tables for the current website contract:

- `agg_mode_daily`
- `agg_mode_hour_daily`
- `agg_line_hour_daily`
- `agg_line_stop_daily`
- `agg_stop_group_daily`
- `agg_stop_post_daily`
- `agg_stop_line_daily`
- `agg_stop_hour_daily`
- `mart_delay_events`
- `mart_trip_reliability`

All aggregate delay tables expose the same core metric shape where applicable:

- `n`
- `mean_delay_seconds`
- `median_delay_seconds`
- `p90_delay_seconds`
- `on_time_rate`
- `early_count`
- `on_time_count`
- `late_count`
- `delay_histogram`

Delay histograms use 12 ordered buckets:

```text
early_over_5m
early_2_to_5m
early_1_to_2m
on_time_early_30_60s
on_time_early_0_30s
on_time_late_0_30s
on_time_late_30_60s
on_time_late_1_to_3m
late_3_to_5m
late_5_to_10m
late_10_to_20m
late_over_20m
```

Hourly tables use the service-day display window `04:00..03:59`; next-day `04:xx` rows are excluded from the selected service date's hourly widgets.

## Metadata Tables

- `export_metadata`: one row with `export_id`, `export_version`, `source_mode`, `exported_at`, source project/dataset identifiers, source row/byte totals, exported table count, and DuckDB file size.
- `export_table_stats`: one row per source or derived serving table with row count, source bytes where applicable, and date range where the table has a primary date field.

The current frontend footer displays `export_metadata.exported_at` and source row count. Showing the full available service-date range from `export_table_stats` is a frontend refinement, not a blocker for the alpha serving artifact.

## Page Coverage

The current frontend routes are backed as follows:

- `/`: `agg_mode_daily`, `agg_mode_hour_daily`, `agg_line_daily`, `agg_stop_post_daily`.
- `/lines/`: `agg_line_daily`.
- `/lines/<line>`: `agg_line_daily`, `agg_line_hour_daily`, `agg_line_stop_daily`, `fct_stop_arrival`, `mart_delay_events`, `mart_trip_reliability`.
- `/stops/`: `dim_stop_group_current`, `agg_stop_group_daily`.
- `/stops/<group>` and `/stops/<group>/<post>`: `dim_stop_post_current`, `agg_stop_post_daily`, `agg_stop_line_daily`, `agg_stop_hour_daily`, `fct_stop_arrival`, `mart_delay_events`.
- `/trips/` and `/schedule/`: `fct_trip`, with traces from trusted `observed` rows in `fct_expected_stop_event`; `/trips/` is canonical and `/schedule/` is the compatibility alias.
- `/trips/<trip_id>`: `fct_trip` and full scheduled stop rows from `fct_expected_stop_event`.
- `/status`: `mart_pipeline_status` plus `export_metadata` footer freshness.

The frontend transforms exported rows into CSS-friendly widget shapes such as histogram bars, timeline ticks, and reliability strips. Those transforms are presentation logic. They should not be treated as fake data when the underlying exported rows are real.

## Current Alpha Gaps

These are intentionally not blockers for the alpha DuckDB artifact:

- Scheduled-but-unobserved trips are not yet represented. `/trips/` shows observed trips from `fct_trip`.
- Trip detail uses `fct_expected_stop_event`, so scheduled stops are explicit as `observed`, `missed`, or `uncertain` rows. `uncertain` can include raw timestamps, but the frontend must not treat them as trusted delay evidence.
- The export does not yet include sanitized private poller heartbeat status. Live poller status should be added separately from the archive-serving baseline.
- The file is not scheduled for daily unattended export yet. Daily export cadence should wait for the cost and matcher-hardening passes.
- Current facts remain provisional until #71, #72, #73, and #20 land the settled matching path and confidence diagnostics.

## Matcher Caveats

Until settled matching lands, frontend analytics must keep these caveats in mind:

- `int_ping_trip` assigns pings only inside the scheduled trip start/end window.
- Early origin departures before scheduled start can be unobservable in `fct_stop_arrival`.
- First-stop delay distributions are censored and must not be interpreted as proof that vehicles never depart early.
- Very delayed trip tails after scheduled end can degrade to partial/broken or be confused with the next scheduled trip before quality flags exclude them.

Strict public analytics should default to `trip_quality = 'complete'`. Exploratory/debug views may include `partial` or `broken` only when they expose quality caveats clearly.

## Follow-Up Ownership

Keep these out of the alpha closure unless explicitly pulled in:

- Daily cost/cadence hardening: #63 and #68.
- Settled matching and expected scheduled events: #71, #72, #73, and #20.
- Poller heartbeat exposure in the frontend-serving layer: follow-up from #37/#40.
