{% set processing_date = var("processing_date", "1970-01-01") %}
{% set scope = serving_route_pattern_rebuild_range() %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        on_schema_change='sync_all_columns',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with {{ serving_route_pattern_window_ctes() }},

arrivals as (
    select *
    from {{ ref('int_serving_stop_arrival') }}
    where service_date between date_sub(date('{{ scope.start }}'), interval {{ lookback_days }} day)
        and date('{{ scope.end }}')
),

calls as (
    select
        eligible.* except (stops),
        stop.*,
        arrivals.delay_seconds,
        to_json_string(struct(
            eligible.line, eligible.mode, eligible.direction_id, eligible.trip_headsign,
            eligible.route_pattern_id, eligible.window_type, eligible.window_key,
            eligible.source_end_date, stop.call_position
        )) as grain_key
    from eligible
    cross join unnest(eligible.stops) as stop
    left join arrivals
        on eligible.gtfs_snapshot_id = arrivals.gtfs_snapshot_id
        and eligible.gps_date = arrivals.gps_date
        and eligible.service_date = arrivals.service_date
        and eligible.trip_id = arrivals.trip_id
        and eligible.vehicle_number = arrivals.vehicle_number
        and stop.stop_sequence = arrivals.stop_sequence
        and stop.stop_id = arrivals.stop_id
),

scheduled as (
    select
        grain_key,
        any_value(line) as line,
        any_value(mode) as mode,
        any_value(route_short_name) as route_short_name,
        any_value(direction_id) as direction_id,
        any_value(trip_headsign) as trip_headsign,
        any_value(route_pattern_id) as route_pattern_id,
        any_value(call_position) as call_position,
        min(stop_sequence) as stop_sequence,
        any_value(stop_group_id) as stop_group_id,
        array_agg(stop_id order by stop_id limit 1)[offset(0)] as stop_id,
        array_agg(stop_post_code order by stop_id limit 1)[offset(0)] as stop_post_code,
        array_agg(distinct stop_post_code order by stop_post_code) as stop_post_codes,
        array_agg(stop_name order by service_date desc, gtfs_snapshot_id desc, trip_id limit 1)[offset(0)] as stop_name,
        'all_observed' as universe_type,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        any_value(source_end_date) as source_end_date,
        count(*) as trip_count
    from calls
    group by grain_key
),

-- Keep null observations out of the sample denominator; scheduled calls survive separately.
counts as (
    select grain_key, {{ serving_delay_count_columns() }}
    from calls
    where delay_seconds is not null
    group by grain_key
),

quantiles as (
    select distinct grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds,
        percentile_cont(delay_seconds, 0.9) over (partition by grain_key) as p90_delay_seconds
    from calls
    where delay_seconds is not null
)

select
    scheduled.* except (grain_key),
    scheduled.call_position as display_rank,
    coalesce(counts.arrival_count, 0) as arrival_count,
    counts.mean_delay_seconds,
    quantiles.median_delay_seconds,
    quantiles.p90_delay_seconds,
    quantiles.p90_delay_seconds - quantiles.median_delay_seconds as delay_spread_seconds,
    coalesce(counts.early_count, 0) as early_count,
    coalesce(counts.on_time_count, 0) as on_time_count,
    coalesce(counts.late_count, 0) as late_count,
    counts.early_rate,
    counts.on_time_rate,
    counts.late_rate,
    counts.delay_histogram,
    coalesce(counts.arrival_count, 0) >= 3 as has_min_sample
from scheduled
left join counts using (grain_key)
left join quantiles using (grain_key)
