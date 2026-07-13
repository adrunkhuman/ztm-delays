{% set publish_service_date = var("publish_service_date", var("processing_date")) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ publish_service_date ~ "')"],
        cluster_by=["line", "trip_id"],
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
),

matcher_expected_events as (
    select
        gtfs_snapshot_id,
        gps_date,
        source_gps_date,
        service_date,
        trip_id,
        vehicle_number,
        stop_sequence,
        observation_status,
        actual_arrival_time,
        delay_seconds,
        uncertainty_evidence
    from {{ source('matcher_input', 'reconstruction_expected_stop_events') }}
    where service_date = date('{{ publish_service_date }}')
      and gps_date between date('{{ publish_service_date }}') and date_add(date('{{ publish_service_date }}'), interval 1 day)
      and gps_date <= date('{{ var("processing_date") }}')
),

scheduled_stops as (
    select
        stop_times.gtfs_snapshot_id,
        trip_spine.service_date,
        stop_times.trip_id,
        stop_times.stop_id,
        substr(stop_times.stop_id, 1, 4) as stop_group_id,
        {{ stop_post_code('stop_times.stop_id') }} as stop_post_code,
        stops.stop_name,
        stops.stop_lat,
        stops.stop_lon,
        stop_times.stop_sequence,
        stop_times.pickup_type,
        stop_times.drop_off_type,
        stop_times.stop_service_class,
        stop_times.stop_execution_class,
        stop_times.classification_confidence,
        stop_times.classification_reason,
        stop_times.classification_evidence,
        stop_times.is_passenger_stop,
        stop_times.are_passenger_boundaries_settled,
        stop_times.first_passenger_stop_sequence,
        stop_times.last_passenger_stop_sequence,
        timestamp_add(
            timestamp(trip_spine.service_date, 'Europe/Warsaw'),
            interval stop_times.arrival_time_seconds second
        ) as scheduled_arrival_time,
        timestamp_add(
            timestamp(trip_spine.service_date, 'Europe/Warsaw'),
            interval stop_times.departure_time_seconds second
        ) as scheduled_departure_time
    from (
        select distinct gtfs_snapshot_id, gps_date, service_date, trip_id
        from trip_facts
    ) as trip_spine
    inner join {{ source('matcher_input', 'reconstruction_stop_semantics') }} as stop_times
        on trip_spine.gtfs_snapshot_id = stop_times.gtfs_snapshot_id
        and trip_spine.gps_date = stop_times.processing_date
        and trip_spine.service_date = stop_times.service_date
        and trip_spine.trip_id = stop_times.trip_id
    inner join {{ ref('stg_gtfs__stops') }} as stops
        on stop_times.gtfs_snapshot_id = stops.gtfs_snapshot_id
        and stop_times.stop_id = stops.stop_id
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
            countif(stop_service_class != 'not_in_passenger_service') as passenger_post_count_for_name,
            count(*) as post_count_for_name
        from (
            select distinct
                stop_group_id,
                gtfs_snapshot_id,
                stop_id,
                stop_name,
                stop_service_class
            from scheduled_stops
        )
        group by stop_group_id, gtfs_snapshot_id, stop_name
    )
    qualify row_number() over (
        partition by stop_group_id, gtfs_snapshot_id
        order by passenger_post_count_for_name desc, post_count_for_name desc, stop_name
    ) = 1
),

calendar_dates as (
    select
        service_date,
        is_holiday
    from {{ ref('dim_date') }}
    where service_date = date('{{ publish_service_date }}')
),

observed_arrivals as (
    select
        gtfs_snapshot_id,
        source_gps_date,
        service_date,
        trip_id,
        vehicle_number,
        stop_sequence,
        actual_arrival_time,
        delay_seconds,
        detection_method,
        stop_match_radius_m,
        stop_distance_m,
        prev_ping_distance_m,
        next_ping_distance_m,
        segment_start_time,
        segment_end_time,
        segment_duration_seconds
    from {{ ref('fct_stop_arrival') }}
    where service_date = date('{{ publish_service_date }}')
),

expected_events as (
    select
        trip_facts.gtfs_snapshot_id,
        trip_facts.gps_date,
        coalesce(observed_arrivals.source_gps_date, matcher_expected_events.source_gps_date) as source_gps_date,
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
        scheduled_stops.stop_id,
        scheduled_stops.stop_group_id,
        scheduled_stops.stop_post_code,
        scheduled_stops.stop_name,
        scheduled_stops.stop_lat,
        scheduled_stops.stop_lon,
        stop_group_names.stop_group_name,
        scheduled_stops.stop_sequence,
        scheduled_stops.pickup_type,
        scheduled_stops.drop_off_type,
        scheduled_stops.stop_service_class,
        scheduled_stops.stop_execution_class,
        scheduled_stops.classification_confidence,
        scheduled_stops.classification_reason,
        scheduled_stops.classification_evidence,
        scheduled_stops.is_passenger_stop,
        scheduled_stops.are_passenger_boundaries_settled,
        scheduled_stops.first_passenger_stop_sequence,
        scheduled_stops.last_passenger_stop_sequence,
        scheduled_stops.scheduled_arrival_time,
        scheduled_stops.scheduled_departure_time,
        matcher_expected_events.actual_arrival_time,
        matcher_expected_events.delay_seconds,
        matcher_expected_events.uncertainty_evidence,
        timestamp_trunc(scheduled_stops.scheduled_arrival_time, hour, 'Europe/Warsaw') as hour_bracket,
        observed_arrivals.detection_method,
        observed_arrivals.stop_match_radius_m,
        observed_arrivals.stop_distance_m,
        observed_arrivals.prev_ping_distance_m,
        observed_arrivals.next_ping_distance_m,
        observed_arrivals.segment_start_time,
        observed_arrivals.segment_end_time,
        observed_arrivals.segment_duration_seconds,
        case
            when matcher_expected_events.observation_status is not null
                then matcher_expected_events.observation_status = 'observed'
            else observed_arrivals.actual_arrival_time is not null
        end as is_observed,
        coalesce(matcher_expected_events.observation_status = 'uncertain', false)
            or trip_facts.trip_quality = 'broken'
            or trip_facts.service_observation_class = 'matching_failure' as is_match_uncertain,
        matcher_expected_events.observation_status as matcher_observation_status
    from trip_facts
    inner join scheduled_stops
        on trip_facts.gtfs_snapshot_id = scheduled_stops.gtfs_snapshot_id
        and trip_facts.service_date = scheduled_stops.service_date
        and trip_facts.trip_id = scheduled_stops.trip_id
    inner join stop_group_names
        on scheduled_stops.gtfs_snapshot_id = stop_group_names.gtfs_snapshot_id
        and scheduled_stops.stop_group_id = stop_group_names.stop_group_id
    inner join calendar_dates
        on trip_facts.service_date = calendar_dates.service_date
    left join observed_arrivals
        on trip_facts.gtfs_snapshot_id = observed_arrivals.gtfs_snapshot_id
        and trip_facts.service_date = observed_arrivals.service_date
        and trip_facts.trip_id = observed_arrivals.trip_id
        and trip_facts.vehicle_number = observed_arrivals.vehicle_number
        and scheduled_stops.stop_sequence = observed_arrivals.stop_sequence
    left join matcher_expected_events
        on trip_facts.gtfs_snapshot_id = matcher_expected_events.gtfs_snapshot_id
        and trip_facts.gps_date = matcher_expected_events.gps_date
        and trip_facts.service_date = matcher_expected_events.service_date
        and trip_facts.trip_id = matcher_expected_events.trip_id
        and trip_facts.vehicle_number = matcher_expected_events.vehicle_number
        and scheduled_stops.stop_sequence = matcher_expected_events.stop_sequence
)

select
    gtfs_snapshot_id,
    gps_date,
    source_gps_date,
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
    is_holiday,
    schedule_day_type,
    schedule_service_ids,
    schedule_version_id,
    trip_quality,
    quality_flags,
    service_observation_class,
    service_observation_flags,
    stop_id,
    stop_group_id,
    stop_post_code,
    stop_name,
    stop_lat,
    stop_lon,
    stop_group_name,
    stop_sequence,
    pickup_type,
    drop_off_type,
    stop_service_class,
    stop_execution_class,
    classification_confidence,
    classification_reason,
    classification_evidence,
    is_passenger_stop,
    are_passenger_boundaries_settled,
    first_passenger_stop_sequence,
    last_passenger_stop_sequence,
    scheduled_arrival_time,
    scheduled_departure_time,
    actual_arrival_time,
    delay_seconds,
    uncertainty_evidence,
    hour_bracket,
    detection_method,
    stop_match_radius_m,
    stop_distance_m,
    prev_ping_distance_m,
    next_ping_distance_m,
    segment_start_time,
    segment_end_time,
    segment_duration_seconds,
    is_observed,
    is_match_uncertain,
    case
        when stop_execution_class in ('technical_prefix', 'technical_suffix', 'technical_trip')
            or stop_service_class = 'not_in_passenger_service'
            then 'not_in_passenger_service'
        when stop_execution_class = 'unknown' or not are_passenger_boundaries_settled then 'uncertain'
        when matcher_observation_status is not null then matcher_observation_status
        when is_observed then 'observed'
        when is_match_uncertain or service_observation_class = 'matching_failure' then 'uncertain'
        when stop_service_class = 'request' then 'skipped_optional'
        else 'missed'
    end as observation_status
from expected_events
