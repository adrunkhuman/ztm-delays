{% set test_service_date = var("publish_service_date", var("processing_date")) %}

with grouped_arrivals as (
    select
        gtfs_snapshot_id,
        service_date,
        trip_id,
        vehicle_number,
        stop_sequence,
        count(*) as row_count,
        countif(
            gtfs_snapshot_id is null
            or gps_date is null
            or source_gps_date is null
            or trip_id is null
            or vehicle_number is null
            or line is null
            or mode is null
            or vehicle_type is null
            or direction_id is null
            or trip_headsign is null
            or day_type is null
            or is_holiday is null
            or schedule_day_type is null
            or schedule_version_id is null
            or trip_quality is null
            or quality_flags is null
            or service_observation_class is null
            or service_observation_flags is null
            or stop_id is null
            or stop_group_id is null
            or stop_post_code is null
            or stop_name is null
            or stop_lat is null
            or stop_lon is null
            or stop_group_name is null
            or stop_sequence is null
            or pickup_type is null
            or drop_off_type is null
            or stop_service_class is null
            or stop_execution_class is null
            or classification_confidence is null
            or classification_reason is null
            or classification_evidence is null
            or are_passenger_boundaries_settled is null
            or scheduled_arrival_time is null
            or actual_arrival_time is null
            or delay_seconds is null
            or hour_bracket is null
            or detection_method is null
            or stop_match_radius_m is null
        ) as required_field_violations,
        countif(
            mode not in ('bus', 'tram', 'metro', 'rail')
            or vehicle_type not in (1, 2)
            or direction_id not in (0, 1)
            or trip_quality not in ('complete', 'partial', 'broken')
            or detection_method not in ('segment_within_75m', 'segment_within_250m')
            or stop_execution_class != 'passenger'
            or not are_passenger_boundaries_settled
        ) as enum_violations,
        countif(delay_seconds != timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)) as delay_violations,
        countif(hour_bracket != timestamp_trunc(scheduled_arrival_time, hour, 'Europe/Warsaw')) as hour_bracket_violations,
        countif(stop_group_id != substr(stop_id, 1, 4)) as stop_group_id_violations
    from {{ ref('fct_stop_arrival') }}
    where service_date = date('{{ test_service_date }}')
    group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
),

contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(enum_violations) as enum_violations,
        sum(delay_violations) as delay_violations,
        sum(hour_bracket_violations) as hour_bracket_violations,
        sum(stop_group_id_violations) as stop_group_id_violations,
        countif(row_count > 1) as duplicate_trip_stop_violations
    from grouped_arrivals
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('enum_accepted_values' as issue_type, enum_violations as violation_count),
    struct('delay_seconds_matches_arrival_delta' as issue_type, delay_violations as violation_count),
    struct('hour_bracket_matches_scheduled_arrival' as issue_type, hour_bracket_violations as violation_count),
    struct('stop_group_id_matches_stop_id' as issue_type, stop_group_id_violations as violation_count),
    struct('unique_trip_stop' as issue_type, duplicate_trip_stop_violations as violation_count)
])
where violation_count > 0
