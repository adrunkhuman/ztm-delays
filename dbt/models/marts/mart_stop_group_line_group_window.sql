{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "stop_group_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with base as (
    select
        stop_group_id,
        mode,
        line,
        route_short_name,
        trip_headsign,
        windows.window_type,
        windows.window_key,
        windows.source_end_date,
        stop_id,
        {{ stop_post_code('stop_id') }} as stop_post_code
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join {{ ref('dim_serving_window_date') }} as windows
        on arrivals.service_date = windows.service_date
        and windows.source_end_date = date('{{ processing_date }}')
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

counts as (
    select
        stop_group_id,
        mode,
        line,
        any_value(route_short_name) as route_short_name,
        trip_headsign,
        window_type,
        window_key,
        source_end_date,
        count(*) as arrival_count
    from base
    group by stop_group_id, mode, line, trip_headsign, window_type, window_key, source_end_date
),

post_candidates as (
    select distinct
        stop_group_id,
        mode,
        line,
        trip_headsign,
        window_type,
        window_key,
        source_end_date,
        stop_id,
        stop_post_code
    from base
),

post_groups as (
    select
        stop_group_id,
        mode,
        line,
        trip_headsign,
        window_type,
        window_key,
        source_end_date,
        array_agg(struct(stop_id, stop_post_code) order by stop_id) as posts
    from post_candidates
    group by stop_group_id, mode, line, trip_headsign, window_type, window_key, source_end_date
),

grouped as (
    select
        counts.stop_group_id,
        counts.mode,
        counts.line,
        counts.route_short_name,
        counts.trip_headsign,
        counts.window_type,
        counts.window_key,
        counts.source_end_date,
        counts.arrival_count,
        post_groups.posts
    from counts
    inner join post_groups using (stop_group_id, mode, line, trip_headsign, window_type, window_key, source_end_date)
)

select
    stop_group_id,
    mode,
    line,
    route_short_name,
    trip_headsign,
    window_type,
    window_key,
    source_end_date,
    dense_rank() over (partition by stop_group_id, mode, window_type, window_key order by safe_cast(line as int64), line) as line_display_rank,
    row_number() over (partition by stop_group_id, mode, line, window_type, window_key order by arrival_count desc, trip_headsign) as destination_display_rank,
    posts
from grouped
