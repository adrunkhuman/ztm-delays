{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["scope_type", "mode", "scope_id"],
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

scoped as (
    select 'line' as scope_type, line as scope_id, * from arrivals
    union all
    select 'stop_group', stop_group_id, * from arrivals
    union all
    select 'stop_post', stop_id, * from arrivals
),

ranked as (
    select
        *,
        row_number() over (partition by scope_type, scope_id, service_date, mode order by delay_seconds desc, scheduled_arrival_time) as delay_rank
    from scoped
)

select
    scope_type,
    scope_id,
    service_date,
    mode,
    line,
    route_short_name,
    trip_id,
    vehicle_number,
    trip_headsign,
    scheduled_arrival_time,
    format_timestamp('%H:%M', scheduled_arrival_time, 'Europe/Warsaw') as time_label,
    stop_group_id,
    stop_id,
    {{ stop_post_code('stop_id') }} as stop_post_code,
    stop_name,
    delay_seconds,
    delay_rank
from ranked
where delay_rank <= 20
