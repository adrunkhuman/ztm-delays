{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode"],
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

counts as (
    select
        service_date,
        mode,
        count(*) as trip_count,
        safe_divide(countif(end_delay_seconds > -60 and end_delay_seconds < 180), count(*)) as on_time_rate
    from trips
    group by service_date, mode
),

quantiles as (
    select distinct
        service_date,
        mode,
        percentile_cont(end_delay_seconds, 0.5) over (partition by service_date, mode) as median_delay_seconds
    from trips
)

select counts.*, quantiles.median_delay_seconds
from counts
inner join quantiles using (service_date, mode)
