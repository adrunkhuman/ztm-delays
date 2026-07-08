{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with base as (
    select *, concat(gtfs_snapshot_id, '|', trip_id, '|', coalesce(vehicle_number, '')) as trip_key
    from {{ ref('fct_stop_arrival') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

keyed as (
    select
        *,
        'all_observed' as universe_type,
        'day' as window_type,
        cast(service_date as string) as window_key,
        service_date as source_end_date,
        min(stop_sequence) over (partition by line, mode, direction_id, trip_headsign, stop_group_id) as display_rank,
        to_json_string(struct(line, mode, direction_id, trip_headsign, stop_group_id)) as grain_key
    from base
),

counts as (
    select
        grain_key,
        any_value(line) as line,
        any_value(mode) as mode,
        any_value(route_short_name) as route_short_name,
        any_value(direction_id) as direction_id,
        any_value(trip_headsign) as trip_headsign,
        min(stop_sequence) as stop_sequence,
        any_value(stop_group_id) as stop_group_id,
        array_agg(stop_id order by stop_id limit 1)[offset(0)] as stop_id,
        array_agg({{ stop_post_code('stop_id') }} order by stop_id limit 1)[offset(0)] as stop_post_code,
        any_value(stop_name) as stop_name,
        any_value(universe_type) as universe_type,
        any_value(window_type) as window_type,
        any_value(window_key) as window_key,
        any_value(source_end_date) as source_end_date,
        min(display_rank) as display_rank,
        count(distinct trip_key) as trip_count,
        {{ serving_delay_count_columns() }}
    from keyed
    group by grain_key
),

quantiles as (
    select distinct grain_key,
        percentile_cont(delay_seconds, 0.5) over (partition by grain_key) as median_delay_seconds,
        percentile_cont(delay_seconds, 0.9) over (partition by grain_key) as p90_delay_seconds
    from keyed
)

select
    counts.line,
    counts.mode,
    counts.route_short_name,
    counts.direction_id,
    counts.trip_headsign,
    counts.stop_sequence,
    counts.stop_group_id,
    counts.stop_id,
    counts.stop_post_code,
    counts.stop_name,
    counts.universe_type,
    counts.window_type,
    counts.window_key,
    counts.source_end_date,
    counts.display_rank,
    counts.arrival_count,
    counts.mean_delay_seconds,
    quantiles.median_delay_seconds,
    quantiles.p90_delay_seconds,
    quantiles.p90_delay_seconds - quantiles.median_delay_seconds as delay_spread_seconds,
    counts.early_count,
    counts.on_time_count,
    counts.late_count,
    counts.early_rate,
    counts.on_time_rate,
    counts.late_rate,
    counts.delay_histogram,
    counts.arrival_count >= 3 as has_min_sample
from counts
inner join quantiles using (grain_key)
