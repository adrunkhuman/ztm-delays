{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["mode", "line", "stop_group_id"],
        require_partition_filter=true,
    )
}}

select arrivals.*
from {{ ref('fct_stop_arrival') }} as arrivals
inner join {{ ref('int_serving_trip_execution') }} as trips
    on arrivals.gtfs_snapshot_id = trips.gtfs_snapshot_id
    and arrivals.service_date = trips.service_date
    and arrivals.trip_id = trips.trip_id
    and arrivals.vehicle_number = trips.vehicle_number
where arrivals.service_date = date('{{ processing_date }}')
  and trips.service_date = date('{{ processing_date }}')
