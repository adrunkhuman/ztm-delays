# Serving Contract

Proposal for the serving-layer redesign. This is an approval gate: do not change dbt models, the DuckDB export, or `frontend/ztm_frontend/queries.py` until this contract is approved.

The frontend reads one artifact and one sidecar:

```text
ztm.duckdb
ztm.duckdb.meta.json
```

DuckDB is a serving artifact, not a mirror of `ztm_marts`. Every displayed statistic must already exist in a warehouse-built mart at display grain. Frontend code may format, filter, route, and map values into CSS shapes. It must not aggregate, rank, compute quantiles, derive health scores, derive trip erratic scores, or build trip delay profiles.

## Rules

- Strict public delay statistics use `trip_quality = 'complete'`.
- Delay is `actual_arrival_time - scheduled_arrival_time`, seconds; positive is late.
- Early is `delay_seconds <= -60`, late is `delay_seconds >= 180`, and on-time is strictly between those bounds.
- Medians and p90s are true BigQuery quantiles at the exact display grain.
- No weighted median/p90 recombination exists in DuckDB or Python.
- Stop group is `LEFT(stop_id, 4)`.
- Stop post labels come from warehouse `stop_post_code`, not frontend string slicing.
- Modes in this contract are GPS-observed `bus` and `tram`.
- Date-partitioned warehouse models keep bounded `insert_overwrite` patterns and partition pruning.

## Shared Columns

Use these names consistently instead of repeating ad-hoc metric shapes.

`window_cols`: `window_type`, `window_key`, `source_start_date`, `source_end_date`, `source_day_count`.

`delay_metric_cols`: `arrival_count`, `trip_count`, `mean_delay_seconds`, `median_delay_seconds`, `p90_delay_seconds`, `delay_spread_seconds`, `early_count`, `on_time_count`, `late_count`, `early_rate`, `on_time_rate`, `late_rate`, `delay_histogram`.

`delay_histogram`: ordered `array<struct<bucket_label, min_delay_seconds, max_delay_seconds, n>>` with these fixed labels: `early_over_5m`, `early_2_to_5m`, `early_1_to_2m`, `on_time_early_30_60s`, `on_time_early_0_30s`, `on_time_late_0_30s`, `on_time_late_30_60s`, `on_time_late_1_to_3m`, `late_3_to_5m`, `late_5_to_10m`, `late_10_to_20m`, `late_over_20m`.

Bucket bounds are, in order: `< -300`, `-300..-121`, `-120..-60`, `-59..-31`, `-30..-1`, `0..30`, `31..60`, `61..179`, `180..300`, `301..600`, `601..1200`, `> 1200`.

`universe_type`: `all_observed` for detail pages, `zone1_public` for rankings.

`rank_eligibility_min_arrivals`: provisional minimum sample constants used before an entity can enter `mart_entity_rankings`. These are serving-quality thresholds, like trip-quality thresholds, not presentation rules.

`entity_type`: `mode`, `line`, `stop_group`, `stop_post`, where supported by the table.

## Windows

| Window | Definition |
| --- | --- |
| `day` | One service date, matching current page filtering. |
| `weekdays` | Schedule service that actually ran as weekday service, over the applicable source window ending at `window_key`. |
| `weekend` | Schedule service that actually ran as Saturday or Sunday/holiday service, over the applicable source window ending at `window_key`. |
| `month` | Calendar month. No schedule-change cutoff. Partial current month is valid. |

Schedule classification comes from GTFS service patterns, not calendar weekday. Before implementation trusts `weekdays`/`weekend`, verify observed `service_id` tokens against `dim_schedule_date.schedule_day_type`.

Initial mapping after audit: `monday`, `tuesday`, `wednesday`, `thursday`, `friday`, `weekday` -> `weekdays`; `saturday`, `sunday_holiday` -> `weekend`; `mixed`, `unknown` excluded until explicitly mapped.

Source-window cutoffs:

- Line-grain windows use the shorter of the last 60 matching service dates or since the line's latest schedule-version change.
- Non-line windows use a flat last 60 matching service dates. This applies to mode, stop-group, and stop-post tables because those entities can combine many lines with different schedule versions.
- `month` ignores schedule-version cutoffs for every entity type.

Schedule change means `schedule_version_id` / timetable fingerprint, not GTFS snapshot ID.

## Ranking Universe

`mart_entity_rankings` ranks only the zone-1 public-service universe.

A trip qualifies when it is public service, is not depot pull-out/pull-in, is not a short-turn or depot-return part-trip, and every scheduled stop it touches has zone `1` or `1/2` semantics. Pure zone-2 stops disqualify the trip.

Lines, stop groups, and stop posts qualify through qualifying trips. Entities outside the universe have no rank row.

Ranking eligibility also requires enough observations. `n_entities` is the count of eligible entities for the same `entity_type`, `metric`, `mode`, `window_type`, and `window_key`.

Provisional minimum arrivals:

| Entity | `day` | `weekdays` / `weekend` | `month` |
| --- | ---: | ---: | ---: |
| `line` | 20 | `20 * source_day_count` | `20 * source_day_count` |
| `stop_group` | 10 | `10 * source_day_count` | `10 * source_day_count` |
| `stop_post` | 10 | `10 * source_day_count` | `10 * source_day_count` |

These constants preserve the old day-level floors while scaling window ranks by included service days. They are intentionally named and provisional so they can be tuned with evidence.

Part-trip detection must be evidenced before rank rows are trusted. Proposed operational rule: for each `gtfs_snapshot_id`, `line`, `direction_id`, and `schedule_day_type`, build canonical public stop patterns from non-depot trips; flag a trip as a short-turn part-trip when its ordered passenger stops are a strict contiguous subsequence of a longer canonical public pattern and it is not the dominant terminal pair for that pattern. Evidence must include counts by line/direction, flagged examples, retained full-trip examples, and depot endpoint examples from `int_gtfs_duty_chain.is_public_service_segment`.

## Table Contract

All listed tables are exported to DuckDB unless explicitly marked sidecar. `Columns` names table-specific columns; shared column sets are referenced by name.

| Table | Grain | Purpose | Columns |
| --- | --- | --- | --- |
| `dim_serving_date` | One available service date; `is_latest` marks the latest complete day per `mart_pipeline_status`, not the newest ingested partial day. | Default date and datebox navigation | `service_date`, `service_date_key`, `previous_service_date`, `next_service_date`, `is_latest`, `service_date_rank_desc` |
| `dim_stop_group_current` | One current stop group | Stop picker only; never relabel historical facts | `stop_group_id`, `stop_group_name`, `modes_served` |
| `dim_stop_post_current` | One current stop post | Current post metadata where needed | `stop_id`, `stop_group_id`, `stop_post_code`, `stop_name`, `modes_served` |
| `mart_mode_window_summary` | `mode`, `window_type`, `window_key` | Overview cards, overview histograms, landing summaries | `mode`, `window_cols`, `line_count`, `stop_group_count`, `stop_post_count`, `delay_metric_cols` |
| `mart_hour_window_summary` | `entity_type`, `entity_id`, `mode`, `window_type`, `window_key`, `local_hour` | Median-by-hour widgets and post-band hour sparklines | `entity_type`, `entity_id`, `mode`, `window_cols`, `local_hour`, `service_hour_index`, `hour_bracket_label`, `arrival_count`, `median_delay_seconds`, `has_min_sample` |
| `mart_entity_daily_summary` | `entity_type`, `entity_id`, `mode`, `service_date` | Widgets labelled `This week`; frontend filters a 7-day range around the selected date | `entity_type`, `entity_id`, `mode`, `service_date`, `arrival_count`, `median_delay_seconds` |
| `mart_line_window_summary` | `line`, `mode`, `universe_type`, `window_type`, `window_key` | Line rail, line landing rows, selected line summary | `line`, `mode`, `route_short_name`, `route_label`, `universe_type`, `window_cols`, `delay_metric_cols` |
| `mart_line_course_window` | `line`, `direction_id`, `trip_headsign`, `universe_type`, `window_type`, `window_key` | Selected-line direction blocks | `line`, `mode`, `route_short_name`, `direction_id`, `trip_headsign`, `universe_type`, `window_cols`, `trip_count`, `course_rank` |
| `mart_line_course_stop_window` | One displayed stop row per line course/window | Selected-line stop lists; no per-course N+1 | `line`, `mode`, `route_short_name`, `direction_id`, `trip_headsign`, `stop_sequence`, `stop_group_id`, `stop_id`, `stop_post_code`, `stop_name`, `universe_type`, `window_type`, `window_key`, `display_rank`, `delay_metric_cols`, `has_min_sample` |
| `mart_stop_group_window_summary` | `stop_group_id`, `mode`, `universe_type`, `window_type`, `window_key` | Stop landing rows and stop group header/summary | `stop_group_id`, `stop_group_name`, `mode`, `universe_type`, `window_cols`, `stop_post_count`, `line_count`, `delay_metric_cols` |
| `mart_stop_post_window_summary` | `stop_id`, `mode`, `universe_type`, `window_type`, `window_key` | Post chooser, post bands, selected post summary | `stop_id`, `stop_group_id`, `stop_post_code`, `stop_name`, `stop_group_name`, `mode`, `universe_type`, `window_cols`, `line_count`, `delay_metric_cols` |
| `mart_stop_post_line_group_window` | `stop_id`, `trip_headsign`, `mode`, `window_type`, `window_key` | Post-band line chips grouped by destination | `stop_id`, `stop_group_id`, `stop_post_code`, `mode`, `trip_headsign`, `window_type`, `window_key`, `display_rank`, `lines` |
| `mart_stop_group_line_group_window` | `stop_group_id`, `line`, `trip_headsign`, `mode`, `window_type`, `window_key` | Stop group `by line` view | `stop_group_id`, `mode`, `line`, `route_short_name`, `trip_headsign`, `window_type`, `window_key`, `line_display_rank`, `destination_display_rank`, `posts` |
| `mart_stop_line_window_summary` | `entity_type`, `entity_id`, `line`, `direction_id`, `trip_headsign`, `window_type`, `window_key` | Selected stop `Lines here - worst first` | `entity_type`, `entity_id`, `stop_group_id`, `stop_id`, `stop_post_code`, `line`, `mode`, `route_short_name`, `direction_id`, `trip_headsign`, `window_type`, `window_key`, `display_rank`, `delay_metric_cols`, `has_min_sample` |
| `mart_entity_rankings` | One eligible ranked entity per metric, mode, and window | Landing ranks and future rank badges | `entity_type`, `entity_id`, `mode`, `window_type`, `window_key`, `metric`, `value`, `rank`, `n_entities` |
| `mart_entity_timeline_daily` | One displayed timeline point | Selected line/stop every-departure timelines | `entity_type`, `entity_id`, `mode`, `service_date`, `point_rank`, `x_percent`, `delay_seconds`, `source_event_time` |
| `mart_worst_delay_event` | One ranked delay event per scope | Worst departure lists | `scope_type`, `scope_id`, `service_date`, `mode`, `line`, `route_short_name`, `trip_id`, `vehicle_number`, `trip_headsign`, `scheduled_arrival_time`, `time_label`, `stop_group_id`, `stop_id`, `stop_post_code`, `stop_name`, `delay_seconds`, `delay_rank` |
| `mart_line_reliability_daily` | `service_date`, `line`, `direction_id`, `trip_headsign` | Line reliability strip | `service_date`, `mode`, `line`, `route_short_name`, `direction_id`, `trip_headsign`, `display_rank`, `clean_count`, `partial_count`, `broken_count`, `outcomes` |
| `mart_trip_mode_daily_summary` | `service_date`, `mode` | Trips landing summary | `service_date`, `mode`, `trip_count`, `median_delay_seconds`, `on_time_rate` |
| `mart_trip_line_daily` | `service_date`, `mode`, `line` | Trip page line picker | `service_date`, `mode`, `line`, `route_short_name`, `trip_count`, `line_display_rank` |
| `mart_trip_daily` | One canonical observed vehicle trip execution | Trip landing rows, selected-line trip rows, trip detail header | `gtfs_snapshot_id`, `service_date`, `gps_date`, `trip_id`, `vehicle_number`, `line`, `route_short_name`, `mode`, `brigade`, `direction_id`, `trip_headsign`, `route_label`, `origin_stop_name`, `destination_stop_name`, `scheduled_start_time`, `scheduled_end_time`, `actual_start_time`, `actual_end_time`, `start_delay_seconds`, `end_delay_seconds`, `stops_expected`, `stops_detected`, `trip_quality`, `delay_profile`, `erratic_score`, `departure_rank`, `line_end_delay_rank`, `line_erratic_rank`, `landing_worst_rank`, `landing_best_rank`, `landing_erratic_rank` |
| `mart_line_trip_group_daily` | `service_date`, `line`, `direction_id`, `trip_headsign` | Selected trip page group headers | `service_date`, `mode`, `line`, `direction_id`, `trip_headsign`, `origin_stop_name`, `destination_stop_name`, `trip_count`, `display_rank` |
| `fct_expected_stop_event` | One scheduled stop per matched vehicle trip | Trip detail stop list only | `service_date`, `trip_id`, `vehicle_number`, `mode`, `line`, `stop_sequence`, `stop_id`, `stop_group_id`, `stop_post_code`, `stop_name`, `scheduled_arrival_time`, `actual_arrival_time`, `delay_seconds`, `observation_status` |
| `mart_pipeline_status` | Operational `service_date`, `mode` | Recent-days status table | `service_date`, `mode`, `completeness_ratio`, `service_coverage_ratio`, `trips_complete`, `trips_partial`, `trips_broken`, `stop_arrivals_count`, `latest_gtfs_snapshot_at`, `health_ratio`, `health_label` |
| `mart_pipeline_status_recent_summary` | One row per mode | Status top panels | `mode`, `day_count`, `first_date`, `last_date`, `completeness_ratio`, `service_coverage_ratio`, `trips_complete`, `trips_broken`, `health_ratio`, `health_label` |
| `export_metadata` | One row | Footer and operator metadata | Display: `source_row_count`, `exported_at`; embedded operational fields are `semantic_validation_status` and `semantic_validation_warnings_json` |
| `export_table_stats` | One row per exported table | Export validation and operator inspection; no visible widget depends on it | `table_name`, `row_count`, `source_size_bytes`, `min_date`, `max_date`, `date_count` |

`mart_entity_rankings.metric` values: `median_delay_seconds`, `on_time_rate`, `arrival_count`, `delay_spread_seconds`. Ranking order is highest value first for all four current UI ranks: worst delay, best on-time, busiest, and most erratic. Rows exist only for entities meeting the `rank_eligibility_min_arrivals` threshold.

`mart_stop_post_line_group_window.lines` is an array of structs containing `line`, `mode`, `route_short_name`.

`mart_stop_group_line_group_window.posts` is an array of structs containing `stop_id`, `stop_post_code`.

`mart_line_reliability_daily.outcomes` is an ordered array of structs containing `trip_id`, `vehicle_number`, `scheduled_start_time`, `outcome`, `label`.

`fct_expected_stop_event` is the only stop-event fact exported. The legacy `fct_scheduled_stop_event` fallback is retired.

## Sidecar Contract

`ztm.duckdb.meta.json` supplies sanitized poller status and bounded semantic-validation results. The frontend displays
poller fields directly; semantic warnings are currently operational metadata.

The sidecar owns the full semantic report below. `export_metadata` embeds only the status and serialized bounded warning
array; checked dates, total warning count, and truncation state are sidecar-only.

| Field | Display use |
| --- | --- |
| `last_export_at` | Status panel export freshness. |
| `poller_status.status` | Poller status label. |
| `poller_status.updated_at` | Heartbeat timestamp. |
| `poller_status.last_success_at` | Last successful poll. |
| `poller_status.vehicle_types.bus.consecutive_failures` | Bus failure count. |
| `poller_status.vehicle_types.tram.consecutive_failures` | Tram failure count. |
| `semantic_validation.status` | Operational `pass` or `warning` result for the published artifact. |
| `semantic_validation.checked_dates` | Changed dates plus the latest serving date checked by bounded fact validation. |
| `semantic_validation.warning_count` | Total warnings found before bounded report truncation. |
| `semantic_validation.warnings_truncated` | True when the sidecar omits warnings beyond the report limit. |
| `semantic_validation.warnings` | Bounded warning records for incomplete but internally consistent source days. |

## Page Mapping

### Global

| Widget | Source |
| --- | --- |
| Footer row count/build time | `export_metadata.source_row_count`, `export_metadata.exported_at`. |
| Date navigation | `dim_serving_date`. |

### Overview `/`

| Widget | Source |
| --- | --- |
| Bus/tram on-time cards | `mart_mode_window_summary`, `window_type = 'day'`. |
| Bus/tram histograms | `mart_mode_window_summary`. |
| Bus/tram median by hour | `mart_hour_window_summary`, `entity_type = 'mode'`. |
| This week network bars | `mart_entity_daily_summary`, `entity_type = 'mode'`; current template renders bus only. |
| Worst lines | `mart_entity_rankings` joined to `mart_line_window_summary`, `entity_type = 'line'`, `metric = 'median_delay_seconds'`, `rank <= 8`, summary `universe_type = 'zone1_public'`. |
| Worst stops | `mart_entity_rankings` joined to `mart_stop_post_window_summary`, `entity_type = 'stop_post'`, `metric = 'median_delay_seconds'`, `rank <= 8`, summary `universe_type = 'zone1_public'`. |

### Lines `/lines/` and `/lines/<line>`

| Widget | Source |
| --- | --- |
| Line rail | `mart_line_window_summary`, `universe_type = 'all_observed'`. |
| Landing summary | `mart_mode_window_summary`. |
| Landing ranks | `mart_entity_rankings` joined to `mart_line_window_summary`, summary `universe_type = 'zone1_public'`; no top-16 cap. |
| Selected line summary/histogram | `mart_line_window_summary`. |
| Selected line hours | `mart_hour_window_summary`, `entity_type = 'line'`. |
| Selected line week bars | `mart_entity_daily_summary`, `entity_type = 'line'`, frontend filters a 7-day range around the selected date. |
| Every departure timeline | `mart_entity_timeline_daily`, `entity_type = 'line'`. |
| Direction headers | `mart_line_course_window`, `course_rank <= 2`. |
| Course stops | `mart_line_course_stop_window`, `display_rank <= 36`, `has_min_sample`. |
| Worst departures | `mart_worst_delay_event`, `scope_type = 'line'`, `delay_rank <= 6`. |
| Reliability strip | `mart_line_reliability_daily`. |

### Stops `/stops/`, `/stops/<group>`, `/stops/<group>/<post>`

| Widget | Source |
| --- | --- |
| Stop picker | `dim_stop_group_current`. |
| Landing summary | `mart_mode_window_summary`. |
| Landing ranks | `mart_entity_rankings` joined to `mart_stop_group_window_summary`, summary `universe_type = 'zone1_public'`; no top-16 cap. |
| Group header | `mart_stop_group_window_summary`. |
| Post chooser and post bands | `mart_stop_post_window_summary`. |
| Post-band hour sparklines | `mart_hour_window_summary`, `entity_type = 'stop_post'`, batched for all posts. |
| Post-band line chips | `mart_stop_post_line_group_window`. |
| By-line view | `mart_stop_group_line_group_window`. |
| Selected post summary/histogram | `mart_stop_post_window_summary`. |
| Selected post hours | `mart_hour_window_summary`, `entity_type = 'stop_post'`. |
| Selected post week bars | `mart_entity_daily_summary`, `entity_type = 'stop_post'`, frontend filters a 7-day range around the selected date. |
| Every departure timeline | `mart_entity_timeline_daily`, `entity_type = 'stop_post'`. |
| Worst departures | `mart_worst_delay_event`, `scope_type = 'stop_post'`, `delay_rank <= 8`. |
| Lines here | `mart_stop_line_window_summary`, `entity_type = 'stop_post'`, `display_rank <= 30`, `has_min_sample`. |

### Trips `/trips/`, `/schedule/`, `/trips/<trip_id>`

| Widget | Source |
| --- | --- |
| Line rail | `mart_trip_line_daily`. |
| Landing summary | `mart_trip_mode_daily_summary`. |
| Landing rank rows | `mart_trip_daily`, selected `landing_*_rank`; no top-16 cap. |
| Selected line group headers | `mart_line_trip_group_daily`. |
| Selected line trip rows | `mart_trip_daily`, ordered by precomputed rank for selected sort. |
| Trip detail header/summary | `mart_trip_daily`. |
| Trip detail stop list | `fct_expected_stop_event`. |

### Status `/status`

| Widget | Source |
| --- | --- |
| Poller panel | `ztm.duckdb.meta.json`. |
| Mode status panels | `mart_pipeline_status_recent_summary`. |
| Recent days table | `mart_pipeline_status`, latest eight rows per mode at frontend read time. |

## Removal Candidates

Retire these current DuckDB tables after replacement marts exist:

| Current table | Replacement |
| --- | --- |
| `agg_line_daily` | `mart_line_window_summary`, `mart_line_course_window`. |
| `agg_mode_daily` | `mart_mode_window_summary`. |
| `agg_mode_hour_daily` | `mart_hour_window_summary`. |
| `agg_line_hour_daily` | `mart_hour_window_summary`. |
| `agg_line_stop_daily` | `mart_line_course_stop_window`. |
| `agg_stop_group_daily` | `mart_stop_group_window_summary`. |
| `agg_stop_post_daily` | `mart_stop_post_window_summary`. |
| `agg_stop_line_daily` | `mart_stop_line_window_summary`, `mart_stop_post_line_group_window`, `mart_stop_group_line_group_window`. |
| `agg_stop_hour_daily` | `mart_hour_window_summary`. |
| `mart_delay_events` | `mart_worst_delay_event`. |
| `mart_trip_reliability` | `mart_line_reliability_daily`. |
| `fct_stop_arrival` | `mart_entity_timeline_daily`; remove unused delay-plot exports. |
| `fct_trip` | `mart_trip_daily`, `mart_trip_mode_daily_summary`, `mart_trip_line_daily`, `mart_line_trip_group_daily`. |

Keep `fct_expected_stop_event`, but export only the trip-detail columns listed above. Remove the `fct_scheduled_stop_event` fallback from frontend code.

Column-level removal candidates:

- Aggregate extras not displayed: `p10_delay_seconds`, `p50_delay_seconds`, `stddev_delay_seconds`, `gtfs_snapshot_ids`.
- Current dimension extras not displayed: zone/locality/coordinate/line-set fields in `dim_stop_group_current` and `dim_stop_post_current`.
- Fact/debug extras not displayed: matcher diagnostics, quality flags, distance diagnostics, segment timings, raw lineage, shape IDs, service IDs, vehicle type.
- `export_table_stats` and non-displayed `export_metadata` fields may stay for operations, but no widget depends on them.

## Expected Diffs

| Area | Reason |
| --- | --- |
| Line summaries/rankings | True line-grain medians and p90s replace weighted means of direction/headsign medians. |
| Stop summaries/rankings | True stop-grain medians and p90s replace recombined lower-grain medians. |
| Week bars | True daily display-grain medians replace weighted lower-grain medians. |
| Landing pages | Full rankings replace `LANDING_ROW_LIMIT = 16`. |
| Ranking rows | Zone-1 public-service universe excludes non-qualifying entities. |
| Night (`N`) lines | Effectively excluded from rankings because `rank_eligibility_min_arrivals` is unmet due to a known midnight-fragmentation bug in trip matching ([#113](https://github.com/adrunkhuman/ztm-pipeline/issues/113)). |
| Best rank ties | Old best ranking used `on_time_rate desc, median_delay_seconds asc`; new ranking is single-metric `on_time_rate` only. |

## Implementation Gates

Before rewiring the frontend, provide evidence for these checks:

| Gate | Evidence |
| --- | --- |
| Query contract | `queries.py` has no `sum(...)`, `count(...)`, `quantile...`, `row_number()`, `group by`, Python `sorted(...)` ranking, or N+1 loops for course stops/post hours. Simple filtering and ordering by precomputed rank are allowed. |
| Stop-event shim | `queries.py` references `fct_expected_stop_event` directly; no `__STOP_EVENT_TABLE__` fallback remains. |
| Quantiles | SQL/dbt tests show medians and p90s are computed at each display grain in BigQuery. |
| Ranking universe | Report shows zone-1 and part-trip filters with examples before rank rows are trusted. |
| Schedule day mapping | Report verifies observed `service_id` tokens and `schedule_day_type` mapping before `weekdays`/`weekend` windows are trusted. |
| Cost invariants | Incremental models keep bounded overwrite and partition pruning. Cost optimization remains separate. |
| Export size sanity | After the first export, report per-table sizes from `export_table_stats`; flag if `mart_entity_timeline_daily` materially exceeds `fct_stop_arrival`-like volume, roughly 10M rows/month bounded by actual arrivals. |
