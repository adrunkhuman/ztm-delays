{% set processing_date = var("processing_date", "1970-01-01") %}
{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "processing_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["gtfs_snapshot_id", "line", "trip_id"],
        require_partition_filter=true,
    )
}}

with scheduled_trips as (
    select
        service_date,
        processing_date,
        schedule_day_type,
        gtfs_snapshot_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_id,
        trip_headsign,
        is_public_service_segment,
        origin_stop_id,
        destination_stop_id,
        stop_count
    from {{ ref('int_gtfs_duty_chain') }}
    where mode in ('bus', 'tram')
      and processing_date = date('{{ processing_date }}')
      and gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
)

select
    scheduled_trips.service_date,
    scheduled_trips.processing_date,
    scheduled_trips.schedule_day_type,
    scheduled_trips.gtfs_snapshot_id,
    scheduled_trips.line,
    scheduled_trips.route_short_name,
    scheduled_trips.mode,
    scheduled_trips.direction_id,
    scheduled_trips.trip_id,
    scheduled_trips.trip_headsign,
    scheduled_trips.is_public_service_segment,
    scheduled_trips.origin_stop_id,
    scheduled_trips.destination_stop_id,
    scheduled_trips.stop_count,
    string_agg(stop_times.stop_id, '|' order by stop_times.stop_sequence) as ordered_stop_ids,
    countif(coalesce(stops.effective_zone_id, '') != '1') as non_zone1_stop_count,
    array_agg(distinct coalesce(stops.effective_zone_id, '') order by coalesce(stops.effective_zone_id, '')) as zone_ids
from scheduled_trips
inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
    on scheduled_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
    and scheduled_trips.trip_id = stop_times.trip_id
    and stop_times.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
left join {{ ref('stg_gtfs__stops') }} as stops
    on scheduled_trips.gtfs_snapshot_id = stops.gtfs_snapshot_id
    and stop_times.stop_id = stops.stop_id
    and stops.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
group by
    scheduled_trips.service_date,
    scheduled_trips.processing_date,
    scheduled_trips.schedule_day_type,
    scheduled_trips.gtfs_snapshot_id,
    scheduled_trips.line,
    scheduled_trips.route_short_name,
    scheduled_trips.mode,
    scheduled_trips.direction_id,
    scheduled_trips.trip_id,
    scheduled_trips.trip_headsign,
    scheduled_trips.is_public_service_segment,
    scheduled_trips.origin_stop_id,
    scheduled_trips.destination_stop_id,
    scheduled_trips.stop_count
