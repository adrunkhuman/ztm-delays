{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "gps_date", "data_type": "date"},
        partitions=["date('" ~ var("processing_date") ~ "')"],
        cluster_by=["line", "trip_id"],
    )
}}

with quality_thresholds as (
    select
        0.80 as complete_stop_ratio,
        0.30 as broken_stop_ratio,
        900 as large_ping_gap_seconds,
        2 as terminal_stop_tolerance,
        120 as terminal_progress_lag_seconds,
        4 as large_stop_sequence_gap,
        50.0 as impossible_speed_mps,
        3600 as extreme_delay_seconds
),

arrival_candidates as (
    select
        gtfs_snapshot_id,
        date('{{ var("processing_date") }}') as gps_date,
        service_date,
        trip_id,
        vehicle_number,
        min(line) as line,
        min(brigade) as brigade,
        min(vehicle_type) as vehicle_type,
        min(day_type) as day_type,
        min(shape_id) as shape_id,
        min(service_id) as service_id,
        min(direction_id) as direction_id,
        count(*) as stops_detected,
        min(stop_sequence) as first_detected_stop_sequence,
        max(stop_sequence) as last_detected_stop_sequence,
        array_agg(actual_arrival_time order by stop_sequence)[offset(0)] as actual_start_time,
        array_agg(actual_arrival_time order by stop_sequence desc)[offset(0)] as actual_end_time
    from {{ ref('int_stop_arrivals') }}
    where gps_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
        and date('{{ var("processing_date") }}')
      and service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
        and date('{{ var("processing_date") }}')
      and stop_service_class = 'regular'
    group by gtfs_snapshot_id, service_date, trip_id, vehicle_number
),

arrival_progression as (
    select
        gtfs_snapshot_id,
        date('{{ var("processing_date") }}') as gps_date,
        service_date,
        trip_id,
        vehicle_number,
        max(coalesce(stop_sequence - previous_stop_sequence, 0)) as max_stop_sequence_gap
    from (
        select
            gtfs_snapshot_id,
            date('{{ var("processing_date") }}') as gps_date,
            service_date,
            trip_id,
            vehicle_number,
            stop_sequence,
            lag(stop_sequence) over (
                partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number
                order by stop_sequence
            ) as previous_stop_sequence
        from {{ ref('int_stop_arrivals') }}
        where gps_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
          and service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
          and stop_service_class = 'regular'
    )
    group by gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
),

arrival_time_progression as (
    select
        gtfs_snapshot_id,
        date('{{ var("processing_date") }}') as gps_date,
        service_date,
        trip_id,
        vehicle_number,
        countif(stop_sequence < previous_stop_sequence) > 0 as has_non_monotonic_stop_progression
    from (
        select
            gtfs_snapshot_id,
            date('{{ var("processing_date") }}') as gps_date,
            service_date,
            trip_id,
            vehicle_number,
            stop_sequence,
            lag(stop_sequence) over (
                partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number
                order by actual_arrival_time, stop_sequence
            ) as previous_stop_sequence
        from {{ ref('int_stop_arrivals') }}
        where gps_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
          and service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
          and stop_service_class = 'regular'
    )
    group by gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
),

ping_segments as (
    select
        gtfs_snapshot_id,
        date('{{ var("processing_date") }}') as gps_date,
        service_date,
        trip_id,
        vehicle_number,
        timestamp_diff(gps_time, previous_gps_time, second) as ping_gap_seconds,
        safe_divide(
            st_distance(st_geogpoint(lon, lat), st_geogpoint(previous_lon, previous_lat)),
            nullif(timestamp_diff(gps_time, previous_gps_time, second), 0)
        ) as speed_mps
    from (
        select
            gtfs_snapshot_id,
            date('{{ var("processing_date") }}') as gps_date,
            service_date,
            trip_id,
            vehicle_number,
            gps_time,
            lat,
            lon,
            lag(gps_time) over trip_vehicle_window as previous_gps_time,
            lag(lat) over trip_vehicle_window as previous_lat,
            lag(lon) over trip_vehicle_window as previous_lon
        from {{ ref('int_ping_trip') }}
        where gps_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
          and service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
            and date('{{ var("processing_date") }}')
        window trip_vehicle_window as (
            partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number
            order by gps_time
        )
    )
    where previous_gps_time is not null
),

ping_diagnostics as (
    select
        gtfs_snapshot_id,
        gps_date,
        service_date,
        trip_id,
        vehicle_number,
        max(ping_gap_seconds) as max_ping_gap_seconds,
        max(speed_mps) as max_speed_mps
    from ping_segments
    group by gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
),

stop_extents as (
    select
        stop_times.gtfs_snapshot_id,
        stop_times.trip_id,
        count(*) as required_stop_count,
        min(stop_times.stop_sequence) as first_required_stop_sequence,
        max(stop_times.stop_sequence) as last_required_stop_sequence,
        array_agg(stop_times.stop_id order by stop_times.stop_sequence)[offset(0)] as origin_stop_id,
        array_agg(stops.stop_name order by stop_times.stop_sequence)[offset(0)] as origin_stop_name,
        array_agg(stop_times.stop_id order by stop_times.stop_sequence desc)[offset(0)] as destination_stop_id,
        array_agg(stops.stop_name order by stop_times.stop_sequence desc)[offset(0)] as destination_stop_name,
        array_agg(stop_times.arrival_time_seconds order by stop_times.stop_sequence)[offset(0)]
            as origin_arrival_time_seconds,
        array_agg(stop_times.arrival_time_seconds order by stop_times.stop_sequence desc)[offset(0)]
            as destination_arrival_time_seconds
    from {{ ref('stg_gtfs__stop_times') }} as stop_times
    inner join {{ ref('stg_gtfs__stops') }} as stops
        on stop_times.stop_id = stops.stop_id
        and stop_times.gtfs_snapshot_id = stops.gtfs_snapshot_id
    where stop_times.gtfs_snapshot_id in (select distinct gtfs_snapshot_id from arrival_candidates)
      and stop_times.stop_service_class = 'regular'
    group by stop_times.gtfs_snapshot_id, stop_times.trip_id
),

summarized as (
    select
        arrivals.gtfs_snapshot_id,
        arrivals.gps_date,
        arrivals.service_date,
        arrivals.trip_id,
        arrivals.vehicle_number,
        arrivals.line,
        routes.route_short_name,
        routes.mode,
        arrivals.brigade,
        arrivals.vehicle_type,
        schedule.direction_id,
        schedule.service_id,
        schedule.trip_headsign,
        schedule.shape_id,
        arrivals.day_type,
        schedule.schedule_day_type,
        schedule.schedule_service_ids,
        schedule_version.schedule_version_id,
        stop_extents.origin_stop_id,
        stop_extents.origin_stop_name,
        stop_extents.destination_stop_id,
        stop_extents.destination_stop_name,
        timestamp_add(timestamp(arrivals.service_date, 'Europe/Warsaw'), interval stop_extents.origin_arrival_time_seconds second)
            as scheduled_start_time,
        timestamp_add(timestamp(arrivals.service_date, 'Europe/Warsaw'), interval stop_extents.destination_arrival_time_seconds second)
            as scheduled_end_time,
        arrivals.actual_start_time,
        arrivals.actual_end_time,
        timestamp_diff(
            arrivals.actual_start_time,
            timestamp_add(timestamp(arrivals.service_date, 'Europe/Warsaw'), interval stop_extents.origin_arrival_time_seconds second),
            second
        ) as start_delay_seconds,
        timestamp_diff(
            arrivals.actual_end_time,
            timestamp_add(timestamp(arrivals.service_date, 'Europe/Warsaw'), interval stop_extents.destination_arrival_time_seconds second),
            second
        ) as end_delay_seconds,
        stop_extents.required_stop_count as stops_expected,
        arrivals.stops_detected,
        safe_divide(arrivals.stops_detected, stop_extents.required_stop_count) as detected_stop_ratio,
        arrivals.first_detected_stop_sequence,
        arrivals.last_detected_stop_sequence,
        coalesce(arrival_progression.max_stop_sequence_gap, 0) as max_stop_sequence_gap,
        coalesce(ping_diagnostics.max_ping_gap_seconds, 0) as max_ping_gap_seconds,
        coalesce(ping_diagnostics.max_speed_mps, 0.0) as max_speed_mps,
        coalesce(
            arrivals.first_detected_stop_sequence <= stop_extents.first_required_stop_sequence
                + quality_thresholds.terminal_stop_tolerance,
            false
        ) as is_first_stop_observed,
        coalesce(
            arrivals.last_detected_stop_sequence >= stop_extents.last_required_stop_sequence
                - quality_thresholds.terminal_stop_tolerance,
            false
        ) as is_last_stop_observed,
        coalesce(arrival_time_progression.has_non_monotonic_stop_progression, false)
            as has_non_monotonic_stop_progression,
        coalesce(ping_diagnostics.max_speed_mps, 0.0) > quality_thresholds.impossible_speed_mps as has_impossible_speed_jump,
        coalesce(
            arrivals.last_detected_stop_sequence < stop_extents.last_required_stop_sequence
                - quality_thresholds.terminal_stop_tolerance
                and timestamp_diff(
                    arrivals.actual_end_time,
                    timestamp_add(
                        timestamp(arrivals.service_date, 'Europe/Warsaw'),
                        interval stop_extents.destination_arrival_time_seconds second
                    ),
                    second
                ) >= -quality_thresholds.terminal_progress_lag_seconds,
            false
        ) as has_stale_stop_progression,
        quality_thresholds.complete_stop_ratio,
        quality_thresholds.broken_stop_ratio,
        quality_thresholds.large_ping_gap_seconds,
        quality_thresholds.large_stop_sequence_gap,
        quality_thresholds.extreme_delay_seconds
    from arrival_candidates as arrivals
    cross join quality_thresholds
    inner join {{ ref('int_gtfs_trip_schedule') }} as schedule
        on arrivals.gps_date = schedule.processing_date
        and arrivals.service_date = schedule.service_date
        and arrivals.gtfs_snapshot_id = schedule.gtfs_snapshot_id
        and arrivals.trip_id = schedule.trip_id
    inner join {{ ref('int_schedule_version') }} as schedule_version
        on schedule.line = schedule_version.line
        and schedule.direction_id = schedule_version.direction_id
        and schedule.schedule_day_type = schedule_version.schedule_day_type
        and schedule.processing_date between schedule_version.valid_from_date
        and coalesce(schedule_version.valid_to_date, date '9999-12-31')
    left join {{ ref('stg_gtfs__routes') }} as routes
        on arrivals.line = routes.route_id
        and arrivals.gtfs_snapshot_id = routes.gtfs_snapshot_id
    left join stop_extents
        on arrivals.gtfs_snapshot_id = stop_extents.gtfs_snapshot_id
        and arrivals.trip_id = stop_extents.trip_id
    left join arrival_progression
        on arrivals.gtfs_snapshot_id = arrival_progression.gtfs_snapshot_id
        and arrivals.gps_date = arrival_progression.gps_date
        and arrivals.service_date = arrival_progression.service_date
        and arrivals.trip_id = arrival_progression.trip_id
        and arrivals.vehicle_number = arrival_progression.vehicle_number
    left join arrival_time_progression
        on arrivals.gtfs_snapshot_id = arrival_time_progression.gtfs_snapshot_id
        and arrivals.gps_date = arrival_time_progression.gps_date
        and arrivals.service_date = arrival_time_progression.service_date
        and arrivals.trip_id = arrival_time_progression.trip_id
        and arrivals.vehicle_number = arrival_time_progression.vehicle_number
    left join ping_diagnostics
        on arrivals.gtfs_snapshot_id = ping_diagnostics.gtfs_snapshot_id
        and arrivals.gps_date = ping_diagnostics.gps_date
        and arrivals.service_date = ping_diagnostics.service_date
        and arrivals.trip_id = ping_diagnostics.trip_id
        and arrivals.vehicle_number = ping_diagnostics.vehicle_number
),

flagged as (
    select
        *,
        array_concat(
            if(not is_first_stop_observed, ['missing_first_stop'], []),
            if(not is_last_stop_observed, ['missing_last_stop'], []),
            if(detected_stop_ratio < complete_stop_ratio, ['low_stop_coverage'], []),
            if(max_ping_gap_seconds > large_ping_gap_seconds, ['large_ping_gap'], []),
            if(has_non_monotonic_stop_progression, ['non_monotonic_stop_progression'], []),
            if(has_impossible_speed_jump, ['impossible_speed_jump'], []),
            if(
                (is_first_stop_observed and abs(start_delay_seconds) > extreme_delay_seconds)
                or (is_last_stop_observed and abs(end_delay_seconds) > extreme_delay_seconds),
                ['extreme_delay'],
                []
            ),
            if(has_stale_stop_progression, ['stale_stop_progression'], []),
            if(
                detected_stop_ratio < broken_stop_ratio
                or max_stop_sequence_gap > large_stop_sequence_gap
                or has_non_monotonic_stop_progression,
                ['likely_wrong_trip_assignment'],
                []
            )
        ) as quality_flags
    from summarized
),

classified as (
    select
        *,
        case
            when detected_stop_ratio < broken_stop_ratio
                or max_stop_sequence_gap > large_stop_sequence_gap
                or max_ping_gap_seconds > large_ping_gap_seconds * 2
                or has_non_monotonic_stop_progression
                or has_impossible_speed_jump
                then 'broken'
            when detected_stop_ratio >= complete_stop_ratio
                and is_first_stop_observed
                and is_last_stop_observed
                and max_ping_gap_seconds <= large_ping_gap_seconds
                then 'complete'
            else 'partial'
        end as trip_quality
    from flagged
)

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
    origin_stop_id,
    origin_stop_name,
    destination_stop_id,
    destination_stop_name,
    scheduled_start_time,
    scheduled_end_time,
    actual_start_time,
    actual_end_time,
    start_delay_seconds,
    end_delay_seconds,
    stops_expected,
    stops_detected,
    detected_stop_ratio,
    first_detected_stop_sequence,
    last_detected_stop_sequence,
    max_stop_sequence_gap,
    max_ping_gap_seconds,
    max_speed_mps,
    is_first_stop_observed,
    is_last_stop_observed,
    has_non_monotonic_stop_progression,
    has_impossible_speed_jump,
    has_stale_stop_progression,
    trip_quality,
    quality_flags
from classified
