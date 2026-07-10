{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line"],
        require_partition_filter=true,
    )
}}

select *
from {{ ref('fct_trip') }}
where service_date = date('{{ processing_date }}')
  and mode in ('bus', 'tram')
qualify row_number() over (
    partition by service_date, trip_id, vehicle_number
    order by
        case trip_quality
            when 'complete' then 3
            when 'partial' then 2
            when 'broken' then 1
            else 0
        end desc,
        gps_date desc,
        actual_end_time desc,
        gtfs_snapshot_id desc
) = 1
