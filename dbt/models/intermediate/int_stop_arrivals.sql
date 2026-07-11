{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["line", "trip_id"],
        require_partition_filter=true,
    )
}}

{% set gtfs_snapshot_id = var("gtfs_snapshot_id") %}

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
        gtfs_snapshot_id,
        service_id,
        direction_id,
        service_date,
        day_type,
        gps_time_seconds,
        st_geogpoint(lon, lat) as gps_point
    from {{ ref('int_ping_trip') }}
    where gps_date = date('{{ var("processing_date") }}')
      and gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
),

matching_thresholds as (
    select
        75.0 as regular_stop_radius_m,
        250.0 as expanded_stop_radius_m,
        2 as terminal_stop_tolerance
),

segments as (
    select
        *,
        lag(gps_time) over trip_vehicle_window as prev_gps_time,
        lag(gps_time_seconds) over trip_vehicle_window as prev_gps_time_seconds,
        lag(gps_point) over trip_vehicle_window as prev_gps_point
    from pings
    window trip_vehicle_window as (
        partition by vehicle_number, service_date, gtfs_snapshot_id, trip_id
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

passenger_stop_times as (
    select
        *,
        min(stop_sequence) over (partition by gtfs_snapshot_id, trip_id) as first_passenger_stop_sequence,
        max(stop_sequence) over (partition by gtfs_snapshot_id, trip_id) as last_passenger_stop_sequence,
        lag(stop_sequence) over (partition by gtfs_snapshot_id, trip_id order by stop_sequence)
            as previous_passenger_stop_sequence,
        lead(stop_sequence) over (partition by gtfs_snapshot_id, trip_id order by stop_sequence)
            as next_passenger_stop_sequence
    from {{ ref('stg_gtfs__stop_times') }}
    where gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
      and stop_service_class != 'not_in_passenger_service'
),

scheduled_stops as (
    select
        stop_times.trip_id,
        stop_times.stop_id,
        stops.stop_name,
        stop_times.stop_sequence,
        stop_times.arrival_time_seconds,
        stop_times.departure_time_seconds,
        stop_times.pickup_type,
        stop_times.drop_off_type,
        stop_times.stop_service_class,
        stop_times.gtfs_snapshot_id,
        stop_times.first_passenger_stop_sequence,
        stop_times.last_passenger_stop_sequence,
        stop_times.previous_passenger_stop_sequence,
        stop_times.next_passenger_stop_sequence,
        st_geogpoint(stops.stop_lon, stops.stop_lat) as stop_point,
        case
            when stop_times.stop_service_class = 'request'
                or stop_times.stop_sequence <= stop_times.first_passenger_stop_sequence + matching_thresholds.terminal_stop_tolerance
                or stop_times.stop_sequence >= stop_times.last_passenger_stop_sequence - matching_thresholds.terminal_stop_tolerance
                then matching_thresholds.expanded_stop_radius_m
            else matching_thresholds.regular_stop_radius_m
        end as stop_match_radius_m
    from passenger_stop_times as stop_times
    inner join {{ ref('stg_gtfs__stops') }} as stops
        on stop_times.stop_id = stops.stop_id
        and stop_times.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and stops.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
    cross join matching_thresholds
    where stop_times.gtfs_snapshot_id = '{{ gtfs_snapshot_id }}'
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
        valid_segments.gtfs_snapshot_id,
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
        scheduled_stops.pickup_type,
        scheduled_stops.drop_off_type,
        scheduled_stops.stop_service_class,
        scheduled_stops.first_passenger_stop_sequence,
        scheduled_stops.last_passenger_stop_sequence,
        scheduled_stops.previous_passenger_stop_sequence,
        scheduled_stops.next_passenger_stop_sequence,
        scheduled_stops.stop_match_radius_m,
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
        and valid_segments.gtfs_snapshot_id = scheduled_stops.gtfs_snapshot_id
    where scheduled_stops.arrival_time_seconds between valid_segments.prev_gps_time_seconds - 1800
        and valid_segments.gps_time_seconds + 1800
      and st_dwithin(valid_segments.gps_segment, scheduled_stops.stop_point, scheduled_stops.stop_match_radius_m)
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
),

distance_selected_crossings as (
    select *
    from estimated_crossings
    qualify row_number() over (
        partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
        order by stop_distance_m, abs(timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)), actual_arrival_time
    ) = 1
),

progression_ranked_crossings as (
    select
        estimated_crossings.*,
        row_number() over (
            partition by
                estimated_crossings.gtfs_snapshot_id,
                estimated_crossings.service_date,
                estimated_crossings.trip_id,
                estimated_crossings.vehicle_number,
                estimated_crossings.stop_sequence
            order by
                case
                    when estimated_crossings.stop_sequence = estimated_crossings.first_passenger_stop_sequence
                        and next_stop.actual_arrival_time is not null
                        and estimated_crossings.actual_arrival_time > next_stop.actual_arrival_time
                        then 1
                    when estimated_crossings.stop_sequence = estimated_crossings.last_passenger_stop_sequence
                        and previous_stop.actual_arrival_time is not null
                        and estimated_crossings.actual_arrival_time < previous_stop.actual_arrival_time
                        then 1
                    else 0
                end,
                case
                    when (
                        estimated_crossings.stop_sequence = estimated_crossings.first_passenger_stop_sequence
                        and next_stop.actual_arrival_time is not null
                    ) or (
                        estimated_crossings.stop_sequence = estimated_crossings.last_passenger_stop_sequence
                        and previous_stop.actual_arrival_time is not null
                    )
                        then abs(timestamp_diff(
                        estimated_crossings.actual_arrival_time,
                        estimated_crossings.scheduled_arrival_time,
                        second
                    ))
                    else 0
                end,
                estimated_crossings.stop_distance_m,
                abs(timestamp_diff(
                    estimated_crossings.actual_arrival_time,
                    estimated_crossings.scheduled_arrival_time,
                    second
                )),
                estimated_crossings.actual_arrival_time
        ) as progression_candidate_rank
    from estimated_crossings
    left join distance_selected_crossings as next_stop
        on estimated_crossings.gtfs_snapshot_id = next_stop.gtfs_snapshot_id
        and estimated_crossings.service_date = next_stop.service_date
        and estimated_crossings.trip_id = next_stop.trip_id
        and estimated_crossings.vehicle_number = next_stop.vehicle_number
        and estimated_crossings.next_passenger_stop_sequence = next_stop.stop_sequence
    left join distance_selected_crossings as previous_stop
        on estimated_crossings.gtfs_snapshot_id = previous_stop.gtfs_snapshot_id
        and estimated_crossings.service_date = previous_stop.service_date
        and estimated_crossings.trip_id = previous_stop.trip_id
        and estimated_crossings.vehicle_number = previous_stop.vehicle_number
        and estimated_crossings.previous_passenger_stop_sequence = previous_stop.stop_sequence
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
    pickup_type,
    drop_off_type,
    stop_service_class,
    scheduled_arrival_time,
    scheduled_departure_time,
    actual_arrival_time,
    timestamp_diff(actual_arrival_time, scheduled_arrival_time, second) as arrival_delay_seconds,
    case
        when stop_match_radius_m = 250.0 then 'segment_within_250m'
        else 'segment_within_75m'
    end as detection_method,
    stop_match_radius_m,
    stop_distance_m,
    prev_ping_distance_m,
    next_ping_distance_m,
    prev_gps_time as segment_start_time,
    gps_time as segment_end_time,
    segment_duration_seconds
from progression_ranked_crossings
where progression_candidate_rank = 1
