{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with grouped as (
    select
        service_date,
        mode,
        line,
        direction_id,
        trip_headsign,
        any_value(origin_stop_name) as origin_stop_name,
        any_value(destination_stop_name) as destination_stop_name,
        count(*) as trip_count
    from {{ ref('fct_trip') }}
    where service_date = date('{{ processing_date }}')
      and trip_quality = 'complete'
      and mode in ('bus', 'tram')
    group by service_date, mode, line, direction_id, trip_headsign
)

select
    *,
    row_number() over (partition by service_date, mode, line order by trip_count desc, direction_id, trip_headsign) as display_rank
from grouped
