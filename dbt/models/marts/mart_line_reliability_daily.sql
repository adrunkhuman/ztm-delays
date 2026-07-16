{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        on_schema_change='append_new_columns',
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with trips as (
    select
        *,
        case
            when trip_quality = 'complete' then 'clean'
            when trip_quality = 'broken' then 'broken'
            else 'partial'
        end as outcome
    from {{ ref('int_serving_trip_execution') }}
    where service_date = date('{{ processing_date }}')
      and mode in ('bus', 'tram')
),

grouped as (
    select
        service_date,
        any_value(schedule_day_type) as schedule_day_type,
        mode,
        line,
        any_value(route_short_name) as route_short_name,
        schedule_version_id,
        direction_id,
        trip_headsign,
        countif(outcome = 'clean') as clean_count,
        countif(outcome = 'partial') as partial_count,
        countif(outcome = 'broken') as broken_count,
        array_agg(struct(trip_id, vehicle_number, scheduled_start_time, outcome, format_timestamp('%H:%M', scheduled_start_time, 'Europe/Warsaw') as label) order by scheduled_start_time, trip_id, vehicle_number limit 160) as outcomes
    from trips
    group by service_date, mode, line, schedule_version_id, direction_id, trip_headsign
)

select
    *,
    row_number() over (partition by service_date, mode, line order by clean_count + partial_count + broken_count desc, direction_id, trip_headsign) as display_rank
from grouped
