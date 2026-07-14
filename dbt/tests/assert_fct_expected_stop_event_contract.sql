{% set test_service_date = var("publish_service_date", var("processing_date")) %}

with grouped_events as (
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
            or service_date is null
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
            or is_passenger_stop is null
            or are_passenger_boundaries_settled is null
            or scheduled_arrival_time is null
            or scheduled_departure_time is null
            or hour_bracket is null
            or is_observed is null
            or is_match_uncertain is null
            or observation_status is null
        ) as required_field_violations,
        countif(
            observation_status not in ('observed', 'missed', 'uncertain', 'skipped_optional', 'not_in_passenger_service')
            or (observation_status = 'observed' and (actual_arrival_time is null or delay_seconds is null))
            or (is_observed != (observation_status = 'observed'))
            or ((actual_arrival_time is null) != (delay_seconds is null))
            or (actual_arrival_time is not null and observation_status not in ('observed', 'uncertain'))
            or (observation_status = 'missed' and (actual_arrival_time is not null or delay_seconds is not null))
            or (observation_status in ('skipped_optional', 'not_in_passenger_service')
                and (actual_arrival_time is not null or delay_seconds is not null))
            or (stop_execution_class in ('technical_prefix', 'technical_suffix', 'technical_trip')
                and observation_status != 'not_in_passenger_service')
            or ((stop_execution_class = 'unknown' or not are_passenger_boundaries_settled)
                and observation_status != 'uncertain')
        ) as status_violations,
        countif(
            actual_arrival_time is not null
            and delay_seconds != timestamp_diff(actual_arrival_time, scheduled_arrival_time, second)
        ) as delay_violations
    from {{ ref('fct_expected_stop_event') }}
    where service_date = date('{{ test_service_date }}')
    group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
),

contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(status_violations) as status_violations,
        sum(delay_violations) as delay_violations,
        countif(row_count > 1) as duplicate_stop_event_violations
    from grouped_events
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('observation_status_valid' as issue_type, status_violations as violation_count),
    struct('observed_delay_matches_arrival_delta' as issue_type, delay_violations as violation_count),
    struct('unique_stop_event' as issue_type, duplicate_stop_event_violations as violation_count)
])
where violation_count > 0
