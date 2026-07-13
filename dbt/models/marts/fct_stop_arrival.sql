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

with trip_facts as (
    select
        gtfs_snapshot_id,
        gps_date,
        service_date,
        trip_id,
        vehicle_number,
        line,
        route_short_name,
        mode,
        brigade,
        vehicle_type,
        direction_id,
        service_id,
        trip_headsign,
        shape_id,
        day_type,
        schedule_day_type,
        schedule_service_ids,
        schedule_version_id,
        trip_quality,
        quality_flags,
        service_observation_class,
        service_observation_flags
    from {{ ref('fct_trip') }}
    where service_date = date('{{ publish_service_date }}')
      and not has_non_monotonic_stop_progression
),

arrivals_raw as (
    select arrivals.*
    from {{ source('matcher_input', 'reconstruction_stop_arrivals') }} as arrivals
    inner join trip_facts
        on arrivals.gtfs_snapshot_id = trip_facts.gtfs_snapshot_id
        and arrivals.service_date = trip_facts.service_date
        and arrivals.trip_id = trip_facts.trip_id
        and arrivals.vehicle_number = trip_facts.vehicle_number
    where arrivals.service_date = date('{{ publish_service_date }}')
      and arrivals.gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
      and arrivals.source_gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
      and arrivals.gps_date <= date('{{ var("processing_date") }}')
      and arrivals.source_gps_date <= date('{{ var("processing_date") }}')
),

arrivals as (
    select *
    from arrivals_raw
    qualify row_number() over (
        partition by service_date, trip_id, vehicle_number, stop_sequence
        order by
            case trip_quality
                when 'complete' then 3
                when 'partial' then 2
                when 'broken' then 1
                else 0
            end desc,
            gps_date desc,
            actual_arrival_time desc,
            gtfs_snapshot_id desc
    ) = 1
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
    trip_facts.gtfs_snapshot_id,
    trip_facts.gps_date,
    arrivals.source_gps_date,
    trip_facts.service_date,
    trip_facts.trip_id,
    trip_facts.vehicle_number,
    trip_facts.line,
    trip_facts.route_short_name,
    trip_facts.mode,
    trip_facts.brigade,
    trip_facts.vehicle_type,
    trip_facts.direction_id,
    trip_facts.service_id,
    trip_facts.trip_headsign,
    trip_facts.shape_id,
    trip_facts.day_type,
    calendar_dates.is_holiday,
    trip_facts.schedule_day_type,
    trip_facts.schedule_service_ids,
    trip_facts.schedule_version_id,
    trip_facts.trip_quality,
    trip_facts.quality_flags,
    trip_facts.service_observation_class,
    trip_facts.service_observation_flags,
    stop_semantics.stop_id,
    stop_semantics.stop_group_id,
    {{ stop_post_code('stop_semantics.stop_id') }} as stop_post_code,
    stops.stop_name,
    stop_semantics.stop_lat,
    stop_semantics.stop_lon,
    stop_group_names.stop_group_name,
    stop_semantics.stop_sequence,
    stop_semantics.pickup_type,
    stop_semantics.drop_off_type,
    stop_semantics.stop_service_class,
    stop_semantics.stop_execution_class,
    stop_semantics.classification_confidence,
    stop_semantics.classification_reason,
    stop_semantics.classification_evidence,
    stop_semantics.are_passenger_boundaries_settled,
    arrivals.scheduled_arrival_time,
    arrivals.scheduled_departure_time,
    arrivals.actual_arrival_time,
    arrivals.delay_seconds,
    timestamp_trunc(arrivals.scheduled_arrival_time, hour, 'Europe/Warsaw') as hour_bracket,
    arrivals.detection_method,
    arrivals.stop_match_radius_m,
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
inner join {{ source('matcher_input', 'reconstruction_stop_semantics') }} as stop_semantics
    on trip_facts.gtfs_snapshot_id = stop_semantics.gtfs_snapshot_id
    and trip_facts.gps_date = stop_semantics.processing_date
    and trip_facts.service_date = stop_semantics.service_date
    and trip_facts.trip_id = stop_semantics.trip_id
    and arrivals.stop_sequence = stop_semantics.stop_sequence
inner join stops
    on stop_semantics.gtfs_snapshot_id = stops.gtfs_snapshot_id
    and stop_semantics.stop_id = stops.stop_id
inner join stop_group_names
    on stops.gtfs_snapshot_id = stop_group_names.gtfs_snapshot_id
    and stop_semantics.stop_group_id = stop_group_names.stop_group_id
inner join calendar_dates
    on arrivals.service_date = calendar_dates.service_date
