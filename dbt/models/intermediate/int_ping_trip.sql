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

with active_trips as (
    select
        trips.trip_id,
        trips.line,
        trips.service_id,
        trips.direction_id,
        trips.brigade,
        trips.shape_id,
        trips.gtfs_snapshot_id,
        calendar_dates.service_date,
        calendar_dates.day_type
    from {{ ref('stg_gtfs__trips') }} as trips
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on trips.service_id = calendar_dates.service_id
        and trips.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
    where calendar_dates.service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
        and date('{{ var("processing_date") }}')
      and trips.gtfs_snapshot_id = '{{ var("gtfs_snapshot_id") }}'
),

gps_pings as (
    select
        line,
        brigade,
        lat,
        lon,
        gps_time,
        vehicle_number,
        vehicle_type,
        ingested_at,
        gps_date
    from {{ ref('stg_gps__pings') }}
    where gps_date = date('{{ var("processing_date") }}')
),

trip_windows as (
    select
        active_trips.trip_id,
        active_trips.line,
        active_trips.service_id,
        active_trips.direction_id,
        active_trips.brigade,
        active_trips.shape_id,
        active_trips.gtfs_snapshot_id,
        active_trips.service_date,
        active_trips.day_type,
        min(least(
            coalesce(stop_times.arrival_time_seconds, stop_times.departure_time_seconds),
            coalesce(stop_times.departure_time_seconds, stop_times.arrival_time_seconds)
        )) as trip_start_seconds,
        max(greatest(
            coalesce(stop_times.arrival_time_seconds, stop_times.departure_time_seconds),
            coalesce(stop_times.departure_time_seconds, stop_times.arrival_time_seconds)
        )) as trip_end_seconds
    from active_trips
    inner join {{ ref('stg_gtfs__stop_times') }} as stop_times
        on active_trips.trip_id = stop_times.trip_id
        and active_trips.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
    group by
        active_trips.trip_id,
        active_trips.line,
        active_trips.service_id,
        active_trips.direction_id,
        active_trips.brigade,
        active_trips.shape_id,
        active_trips.gtfs_snapshot_id,
        active_trips.service_date,
        active_trips.day_type
),

candidate_matches as (
    select
        gps.line,
        gps.brigade,
        gps.lat,
        gps.lon,
        gps.gps_time,
        gps.vehicle_number,
        gps.vehicle_type,
        gps.ingested_at,
        gps.gps_date,
        trip_windows.trip_id,
        trip_windows.shape_id,
        trip_windows.gtfs_snapshot_id,
        trip_windows.service_id,
        trip_windows.direction_id,
        trip_windows.service_date,
        trip_windows.day_type,
        trip_windows.trip_start_seconds,
        trip_windows.trip_end_seconds,
        timestamp_diff(gps.gps_time, timestamp(trip_windows.service_date, 'Europe/Warsaw'), second) as gps_time_seconds
    from gps_pings as gps
    inner join trip_windows
        on gps.line = trip_windows.line
        and gps.brigade = trip_windows.brigade
        and timestamp_diff(gps.gps_time, timestamp(trip_windows.service_date, 'Europe/Warsaw'), second)
        between trip_windows.trip_start_seconds and trip_windows.trip_end_seconds
)

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
    trip_start_seconds,
    trip_end_seconds,
    gps_time_seconds
from candidate_matches
qualify row_number() over (
    partition by vehicle_number, gps_time
    order by timestamp_add(timestamp(service_date, 'Europe/Warsaw'), interval trip_start_seconds second) desc, trip_id
) = 1
