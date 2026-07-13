{% set publish_service_date = var("publish_service_date", var("processing_date")) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ publish_service_date ~ "')"],
        cluster_by=["line", "direction_id"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

{% if var('use_python_reconstruction', false) %}
with matcher_trip_candidates as (
    select *
    from {{ source('matcher_input', 'reconstruction_trip_facts') }}
    where service_date = date('{{ publish_service_date }}')
      and gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
      and gps_date <= date('{{ var("processing_date") }}')
),

matcher_trips as (
    select *
    from matcher_trip_candidates
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
),

passenger_extents as (
    select
        matcher.gtfs_snapshot_id,
        matcher.gps_date,
        matcher.service_date,
        matcher.trip_id,
        matcher.vehicle_number,
        array_agg(expected.stop_id order by expected.stop_sequence)[offset(0)] as origin_stop_id,
        array_agg(stops.stop_name order by expected.stop_sequence)[offset(0)] as origin_stop_name,
        array_agg(expected.stop_id order by expected.stop_sequence desc)[offset(0)] as destination_stop_id,
        array_agg(stops.stop_name order by expected.stop_sequence desc)[offset(0)] as destination_stop_name
    from matcher_trips as matcher
    inner join {{ source('matcher_input', 'reconstruction_expected_stop_events') }} as expected
        on matcher.gtfs_snapshot_id = expected.gtfs_snapshot_id
        and matcher.gps_date = expected.gps_date
        and matcher.service_date = expected.service_date
        and matcher.trip_id = expected.trip_id
        and matcher.vehicle_number = expected.vehicle_number
    inner join {{ ref('stg_gtfs__stops') }} as stops
        on expected.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and expected.stop_id = stops.stop_id
    group by matcher.gtfs_snapshot_id, matcher.gps_date, matcher.service_date, matcher.trip_id, matcher.vehicle_number
),

enriched as (
    select
        matcher.gtfs_snapshot_id,
        matcher.gps_date,
        matcher.service_date,
        matcher.trip_id,
        matcher.vehicle_number,
        schedule.line,
        routes.route_short_name,
        routes.mode,
        matcher.brigade,
        case routes.mode
            when 'bus' then 1
            when 'tram' then 2
        end as vehicle_type,
        schedule.direction_id,
        schedule.service_id,
        schedule.trip_headsign,
        schedule.shape_id,
        calendar_dates.day_type,
        schedule.schedule_day_type,
        schedule.schedule_service_ids,
        schedule_version.schedule_version_id,
        passenger_extents.origin_stop_id,
        passenger_extents.origin_stop_name,
        passenger_extents.destination_stop_id,
        passenger_extents.destination_stop_name,
        matcher.scheduled_start_time,
        matcher.scheduled_end_time,
        matcher.actual_start_time,
        matcher.actual_end_time,
        matcher.start_delay_seconds,
        matcher.end_delay_seconds,
        matcher.passenger_stops_expected as stops_expected,
        matcher.passenger_stops_detected as stops_detected,
        matcher.detected_stop_ratio,
        matcher.first_detected_stop_sequence,
        matcher.last_detected_stop_sequence,
        matcher.max_stop_sequence_gap,
        matcher.max_ping_gap_seconds,
        matcher.max_speed_mps,
        matcher.is_first_stop_observed,
        matcher.is_last_stop_observed,
        matcher.has_non_monotonic_stop_progression,
        matcher.has_impossible_speed_jump,
        matcher.has_stale_stop_progression,
        matcher.trip_quality,
        matcher.quality_flags,
        matcher.service_observation_class,
        matcher.service_observation_flags
    from matcher_trips as matcher
    inner join {{ ref('int_gtfs_trip_schedule_history') }} as schedule
        on matcher.gtfs_snapshot_id = schedule.gtfs_snapshot_id
        and matcher.gps_date = schedule.processing_date
        and matcher.service_date = schedule.service_date
        and matcher.trip_id = schedule.trip_id
    inner join {{ ref('stg_gtfs__routes') }} as routes
        on schedule.gtfs_snapshot_id = routes.gtfs_snapshot_id
        and schedule.line = routes.route_id
    inner join {{ ref('dim_date') }} as calendar_dates
        on matcher.service_date = calendar_dates.service_date
    inner join {{ ref('dim_schedule_version') }} as schedule_version
        on schedule.line = schedule_version.line
        and schedule.direction_id = schedule_version.direction_id
        and schedule.schedule_day_type = schedule_version.schedule_day_type
        and schedule.processing_date between schedule_version.valid_from_date
            and coalesce(schedule_version.valid_to_date, date '9999-12-31')
    inner join passenger_extents
        on matcher.gtfs_snapshot_id = passenger_extents.gtfs_snapshot_id
        and matcher.gps_date = passenger_extents.gps_date
        and matcher.service_date = passenger_extents.service_date
        and matcher.trip_id = passenger_extents.trip_id
        and matcher.vehicle_number = passenger_extents.vehicle_number
    where matcher.scheduled_end_time < timestamp(date_add(date('{{ var("processing_date") }}'), interval 1 day), 'Europe/Warsaw')
)

select *
from enriched
{% else %}
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
    quality_flags,
    service_observation_class,
    service_observation_flags
from {{ ref('int_trip_summary') }}
where service_date = date('{{ publish_service_date }}')
  and gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
  and gps_date <= date('{{ var("processing_date") }}')
  and scheduled_end_time < timestamp(date_add(date('{{ var("processing_date") }}'), interval 1 day), 'Europe/Warsaw')
qualify row_number() over (
    partition by gtfs_snapshot_id, service_date, trip_id, vehicle_number
    order by
        case trip_quality
            when 'complete' then 3
            when 'partial' then 2
            when 'broken' then 1
            else 0
        end desc,
        gps_date desc,
        actual_end_time desc
) = 1
{% endif %}
