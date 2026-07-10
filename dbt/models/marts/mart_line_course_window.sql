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

with trips as (
    select *
    from {{ ref('int_serving_trip_execution') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
),

grouped as (
    select
        line,
        mode,
        any_value(route_short_name) as route_short_name,
        direction_id,
        trip_headsign,
        'all_observed' as universe_type,
        'day' as window_type,
        cast(service_date as string) as window_key,
        service_date as source_end_date,
        count(*) as trip_count
    from trips
    group by line, mode, direction_id, trip_headsign, service_date
)

select
    *,
    row_number() over (partition by line, mode, universe_type, window_type, window_key order by trip_count desc, direction_id, trip_headsign) as course_rank
from grouped
