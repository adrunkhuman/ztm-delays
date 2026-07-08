{% set processing_date = var("processing_date", "1970-01-01") %}

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
        stop_id,
        stop_group_id,
        mode,
        trip_headsign,
        service_date,
        {{ stop_post_code('stop_id') }} as stop_post_code,
        line,
        route_short_name
    from {{ ref('fct_stop_arrival') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

counts as (
    select
        stop_id,
        stop_group_id,
        any_value(stop_post_code) as stop_post_code,
        mode,
        trip_headsign,
        service_date,
        'day' as window_type,
        cast(service_date as string) as window_key,
        service_date as source_end_date,
        count(*) as arrival_count
    from base
    group by stop_id, stop_group_id, mode, trip_headsign, service_date
),

line_candidates as (
    select distinct
        stop_id,
        stop_group_id,
        mode,
        trip_headsign,
        service_date,
        line,
        route_short_name
    from base
),

line_groups as (
    select
        stop_id,
        stop_group_id,
        mode,
        trip_headsign,
        service_date,
        array_agg(struct(line, mode, route_short_name) order by safe_cast(line as int64), line) as lines
    from line_candidates
    group by stop_id, stop_group_id, mode, trip_headsign, service_date
),

grouped as (
    select
        counts.stop_id,
        counts.stop_group_id,
        counts.stop_post_code,
        counts.mode,
        counts.trip_headsign,
        counts.window_type,
        counts.window_key,
        counts.source_end_date,
        counts.arrival_count,
        line_groups.lines
    from counts
    inner join line_groups using (stop_id, stop_group_id, mode, trip_headsign, service_date)
)

select
    stop_id,
    stop_group_id,
    stop_post_code,
    mode,
    trip_headsign,
    window_type,
    window_key,
    source_end_date,
    row_number() over (partition by stop_id, mode, window_type, window_key order by arrival_count desc, trip_headsign) as display_rank,
    lines
from grouped
