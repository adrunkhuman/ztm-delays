{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["entity_type", "mode", "entity_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with arrivals as (
    select *
    from {{ ref('int_serving_stop_arrival') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

line_points as (
    select
        'line' as entity_type,
        line as entity_id,
        mode,
        service_date,
        percentile_cont(delay_seconds, 0.5) over (partition by line, mode, service_date, trip_id, vehicle_number) as delay_seconds,
        timestamp_millis(cast((unix_millis(min(scheduled_arrival_time) over (partition by line, mode, service_date, trip_id, vehicle_number)) + unix_millis(max(scheduled_arrival_time) over (partition by line, mode, service_date, trip_id, vehicle_number))) / 2 as int64)) as source_event_time
    from arrivals
    qualify row_number() over (partition by line, mode, service_date, trip_id, vehicle_number order by scheduled_arrival_time) = 1
),

stop_points as (
    select 'stop_group' as entity_type, stop_group_id as entity_id, mode, service_date, delay_seconds, scheduled_arrival_time as source_event_time from arrivals
    union all
    select 'stop_post', stop_id, mode, service_date, delay_seconds, scheduled_arrival_time from arrivals
),

points as (
    select * from line_points
    union all
    select * from stop_points
),

positioned as (
    select
        *,
        (unix_millis(source_event_time) - unix_millis(timestamp_add(timestamp(service_date, 'Europe/Warsaw'), interval 4 hour)))
            / (24 * 60 * 60 * 1000) * 100 as x_percent,
        row_number() over (partition by entity_type, entity_id, mode, service_date order by source_event_time) as point_rank
    from points
)

select
    entity_type,
    entity_id,
    mode,
    service_date,
    point_rank,
    x_percent,
    delay_seconds,
    source_event_time
from positioned
where point_rank <= 600
