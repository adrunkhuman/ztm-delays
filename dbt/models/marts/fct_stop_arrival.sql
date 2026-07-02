{% set publish_service_date = var("publish_service_date", var("processing_date")) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ publish_service_date ~ "')"],
        cluster_by=["line", "stop_group_id", "hour_bracket"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with arrivals as (
    select
        gtfs_snapshot_id,
        gps_date as source_gps_date,
        service_date,
        trip_id,
        vehicle_number,
        line,
        brigade,
        vehicle_type,
        shape_id,
        service_id,
        direction_id,
        stop_id,
        substr(stop_id, 1, 4) as stop_group_id,
        stop_sequence,
        scheduled_arrival_time,
        scheduled_departure_time,
        actual_arrival_time,
        arrival_delay_seconds as delay_seconds,
        timestamp_trunc(scheduled_arrival_time, hour, 'Europe/Warsaw') as hour_bracket,
        detection_method,
        stop_distance_m,
        prev_ping_distance_m,
        next_ping_distance_m,
        segment_start_time,
        segment_end_time,
        segment_duration_seconds
    from {{ ref('int_stop_arrivals') }}
    where service_date = date('{{ publish_service_date }}')
      and gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
      and gps_date <= date('{{ var("processing_date") }}')
    qualify row_number() over (
        partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
        order by stop_distance_m, abs(arrival_delay_seconds), actual_arrival_time, gps_date desc
    ) = 1
),

trip_facts as (
    select
        gtfs_snapshot_id,
        gps_date,
        service_date,
        trip_id,
        vehicle_number,
        route_short_name,
        mode,
        trip_headsign,
        day_type,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        trip_quality,
        quality_flags
    from {{ ref('fct_trip') }}
    where service_date = date('{{ publish_service_date }}')
),

stops as (
    select
        stop_id,
        substr(stop_id, 1, 4) as stop_group_id,
        stop_name,
        stop_lat,
        stop_lon,
        gtfs_snapshot_id
    from {{ ref('stg_gtfs__stops') }}
    where gtfs_snapshot_id in (select distinct gtfs_snapshot_id from arrivals)
),

stop_group_names as (
    select
        stop_group_id,
        gtfs_snapshot_id,
        stop_name as stop_group_name
    from (
        select
            stop_group_id,
            gtfs_snapshot_id,
            stop_name,
            count(*) as post_count_for_name
        from stops
        group by stop_group_id, gtfs_snapshot_id, stop_name
    )
    qualify row_number() over (
        partition by stop_group_id, gtfs_snapshot_id
        order by post_count_for_name desc, stop_name
    ) = 1
),

calendar_dates as (
    select
        service_date,
        is_holiday
    from {{ ref('dim_date') }}
    where service_date = date('{{ publish_service_date }}')
)

select
    arrivals.gtfs_snapshot_id,
    trip_facts.gps_date,
    arrivals.source_gps_date,
    arrivals.service_date,
    arrivals.trip_id,
    arrivals.vehicle_number,
    arrivals.line,
    trip_facts.route_short_name,
    trip_facts.mode,
    arrivals.brigade,
    arrivals.vehicle_type,
    arrivals.direction_id,
    arrivals.service_id,
    trip_facts.trip_headsign,
    arrivals.shape_id,
    trip_facts.day_type,
    calendar_dates.is_holiday,
    trip_facts.schedule_day_type,
    trip_facts.schedule_service_ids,
    trip_facts.schedule_version_id,
    trip_facts.trip_quality,
    trip_facts.quality_flags,
    arrivals.stop_id,
    arrivals.stop_group_id,
    stops.stop_name,
    stops.stop_lat,
    stops.stop_lon,
    stop_group_names.stop_group_name,
    arrivals.stop_sequence,
    arrivals.scheduled_arrival_time,
    arrivals.scheduled_departure_time,
    arrivals.actual_arrival_time,
    arrivals.delay_seconds,
    arrivals.hour_bracket,
    arrivals.detection_method,
    arrivals.stop_distance_m,
    arrivals.prev_ping_distance_m,
    arrivals.next_ping_distance_m,
    arrivals.segment_start_time,
    arrivals.segment_end_time,
    arrivals.segment_duration_seconds
from arrivals
inner join trip_facts
    on arrivals.gtfs_snapshot_id = trip_facts.gtfs_snapshot_id
    and arrivals.service_date = trip_facts.service_date
    and arrivals.trip_id = trip_facts.trip_id
    and arrivals.vehicle_number = trip_facts.vehicle_number
inner join stops
    on arrivals.gtfs_snapshot_id = stops.gtfs_snapshot_id
    and arrivals.stop_id = stops.stop_id
inner join stop_group_names
    on stops.gtfs_snapshot_id = stop_group_names.gtfs_snapshot_id
    and stops.stop_group_id = stop_group_names.stop_group_id
inner join calendar_dates
    on arrivals.service_date = calendar_dates.service_date
