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
        schedule.trip_id,
        schedule.line,
        schedule.service_id,
        schedule.direction_id,
        trips.brigade,
        schedule.shape_id,
        schedule.gtfs_snapshot_id,
        schedule.service_date,
        schedule.trip_start_seconds,
        schedule.trip_end_seconds,
        calendar_dates.day_type
    from {{ ref('int_gtfs_trip_schedule') }} as schedule
    inner join {{ ref('stg_gtfs__trips') }} as trips
        on schedule.trip_id = trips.trip_id
        and schedule.gtfs_snapshot_id = trips.gtfs_snapshot_id
    inner join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on schedule.service_id = calendar_dates.service_id
        and schedule.service_date = calendar_dates.service_date
        and schedule.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
    where schedule.processing_date = date('{{ var("processing_date") }}')
      and schedule.service_date between date_sub(date('{{ var("processing_date") }}'), interval 1 day)
        and date('{{ var("processing_date") }}')
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
        active_trips.trip_id,
        active_trips.shape_id,
        active_trips.gtfs_snapshot_id,
        active_trips.service_id,
        active_trips.direction_id,
        active_trips.service_date,
        active_trips.day_type,
        active_trips.trip_start_seconds,
        active_trips.trip_end_seconds,
        timestamp_diff(gps.gps_time, timestamp(active_trips.service_date, 'Europe/Warsaw'), second) as gps_time_seconds
    from gps_pings as gps
    inner join active_trips
        on gps.line = active_trips.line
        and gps.brigade = active_trips.brigade
        and timestamp_diff(gps.gps_time, timestamp(active_trips.service_date, 'Europe/Warsaw'), second)
        between active_trips.trip_start_seconds and active_trips.trip_end_seconds
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
