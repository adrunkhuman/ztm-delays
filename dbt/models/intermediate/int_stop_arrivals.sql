{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["line", "trip_id"],
    )
}}

with pings as (
    select
        line,
        brigade,
        lat,
        lon,
        gps_time,
        vehicle_number,
        vehicle_type,
        ingested_at,
        gps_date,
        trip_id,
        shape_id,
        service_id,
        direction_id,
        service_date,
        day_type,
        gps_time_seconds,
        st_geogpoint(lon, lat) as gps_point
    from {{ ref('int_ping_trip') }}
    where gps_date = date('{{ var("processing_date") }}')
),

segments as (
    select
        *,
        lag(gps_time) over trip_vehicle_window as prev_gps_time,
        lag(gps_time_seconds) over trip_vehicle_window as prev_gps_time_seconds,
        lag(gps_point) over trip_vehicle_window as prev_gps_point
    from pings
    window trip_vehicle_window as (
        partition by vehicle_number, service_date, trip_id
        order by gps_time
    )
),

valid_segments as (
    select
        *,
        timestamp_diff(gps_time, prev_gps_time, second) as segment_duration_seconds,
        st_makeline(prev_gps_point, gps_point) as gps_segment
    from segments
    where prev_gps_time is not null
      and timestamp_diff(gps_time, prev_gps_time, second) between 1 and 180
),

scheduled_stops as (
    select
        stop_times.trip_id,
        stop_times.stop_id,
        stops.stop_name,
        stop_times.stop_sequence,
        stop_times.arrival_time_seconds,
        stop_times.departure_time_seconds,
        stop_times.gtfs_snapshot_id,
        st_geogpoint(stops.stop_lon, stops.stop_lat) as stop_point
    from {{ ref('stg_gtfs_stop_times') }} as stop_times
    inner join {{ ref('stg_gtfs_stops') }} as stops
        on stop_times.stop_id = stops.stop_id
        and stop_times.gtfs_snapshot_id = stops.gtfs_snapshot_id
),

candidate_crossings as (
    select
        valid_segments.line,
        valid_segments.brigade,
        valid_segments.vehicle_number,
        valid_segments.vehicle_type,
        valid_segments.gps_date,
        valid_segments.trip_id,
        valid_segments.shape_id,
        valid_segments.service_id,
        valid_segments.direction_id,
        valid_segments.service_date,
        valid_segments.day_type,
        valid_segments.prev_gps_time,
        valid_segments.gps_time,
        valid_segments.prev_gps_time_seconds,
        valid_segments.gps_time_seconds,
        valid_segments.segment_duration_seconds,
        scheduled_stops.stop_id,
        scheduled_stops.stop_name,
        scheduled_stops.stop_sequence,
        scheduled_stops.arrival_time_seconds,
        scheduled_stops.departure_time_seconds,
        scheduled_stops.gtfs_snapshot_id,
        timestamp_add(
            timestamp(valid_segments.service_date, 'Europe/Warsaw'),
            interval scheduled_stops.arrival_time_seconds second
        ) as scheduled_arrival_time,
        timestamp_add(
            timestamp(valid_segments.service_date, 'Europe/Warsaw'),
            interval scheduled_stops.departure_time_seconds second
        ) as scheduled_departure_time,
        round(st_distance(valid_segments.gps_segment, scheduled_stops.stop_point), 2) as stop_distance_m,
        round(st_distance(valid_segments.prev_gps_point, scheduled_stops.stop_point), 2) as prev_ping_distance_m,
        round(st_distance(valid_segments.gps_point, scheduled_stops.stop_point), 2) as next_ping_distance_m
    from valid_segments
    inner join scheduled_stops
        on valid_segments.trip_id = scheduled_stops.trip_id
    where scheduled_stops.arrival_time_seconds between valid_segments.prev_gps_time_seconds - 1800
        and valid_segments.gps_time_seconds + 1800
      and st_dwithin(valid_segments.gps_segment, scheduled_stops.stop_point, 50)
),

estimated_crossings as (
    select
        *,
        timestamp_add(
            prev_gps_time,
            interval cast(round(
                segment_duration_seconds
                * coalesce(safe_divide(prev_ping_distance_m, nullif(prev_ping_distance_m + next_ping_distance_m, 0)), 0.5)
            ) as int64) second
        ) as actual_arrival_time
    from candidate_crossings
)

select
    gtfs_snapshot_id,
    line,
    brigade,
    vehicle_number,
    vehicle_type,
    gps_date,
    service_date,
    day_type,
    trip_id,
    shape_id,
    service_id,
    direction_id,
    stop_id,
    stop_name,
    stop_sequence,
    scheduled_arrival_time,
    scheduled_departure_time,
    actual_arrival_time,
    timestamp_diff(actual_arrival_time, scheduled_arrival_time, second) as arrival_delay_seconds,
    'segment_within_50m' as detection_method,
    stop_distance_m,
    prev_ping_distance_m,
    next_ping_distance_m,
    prev_gps_time as segment_start_time,
    gps_time as segment_end_time,
    segment_duration_seconds
from estimated_crossings
qualify row_number() over (
    partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
    order by stop_distance_m, abs(timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)), actual_arrival_time
) = 1
