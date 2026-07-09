with grouped_trips as (
    select
        gtfs_snapshot_id,
        gps_date,
        service_date,
        trip_id,
        vehicle_number,
        count(*) as row_count,
        countif(
            gtfs_snapshot_id is null
            or service_date is null
            or trip_id is null
            or vehicle_number is null
            or line is null
            or brigade is null
            or vehicle_type is null
            or direction_id is null
            or service_id is null
            or trip_headsign is null
            or shape_id is null
            or schedule_day_type is null
            or schedule_version_id is null
            or scheduled_start_time is null
            or scheduled_end_time is null
            or actual_start_time is null
            or actual_end_time is null
            or start_delay_seconds is null
            or end_delay_seconds is null
            or stops_expected is null
            or stops_detected is null
            or detected_stop_ratio is null
            or first_detected_stop_sequence is null
            or last_detected_stop_sequence is null
            or max_stop_sequence_gap is null
            or max_ping_gap_seconds is null
            or max_speed_mps is null
            or is_first_stop_observed is null
            or is_last_stop_observed is null
            or has_non_monotonic_stop_progression is null
            or has_impossible_speed_jump is null
            or has_stale_stop_progression is null
            or trip_quality is null
            or quality_flags is null
            or service_observation_class is null
            or service_observation_flags is null
        ) as required_field_violations,
        countif(trip_quality not in ('complete', 'partial', 'broken')) as trip_quality_violations,
        countif(service_observation_class not in ('regular', 'truncated', 'modified', 'matching_failure'))
            as service_observation_class_violations,
        countif(exists(
            select 1
            from unnest(quality_flags) as quality_flag
            where quality_flag not in (
                'missing_first_stop',
                'missing_last_stop',
                'low_stop_coverage',
                'large_ping_gap',
                'non_monotonic_stop_progression',
                'impossible_speed_jump',
                'large_stop_sequence_gap',
                'extreme_delay',
                'stale_stop_progression',
                'likely_wrong_trip_assignment'
            )
        )) as quality_flag_violations,
        countif(exists(
            select 1
            from unnest(service_observation_flags) as service_observation_flag
            where service_observation_flag not in (
                'short_start',
                'short_end',
                'large_internal_gap',
                'stale_progress',
                'bad_assignment_evidence'
            )
        )) as service_observation_flag_violations
    from {{ ref('int_trip_summary') }}
    where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
    group by gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
),

contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        sum(trip_quality_violations) as trip_quality_violations,
        sum(service_observation_class_violations) as service_observation_class_violations,
        sum(quality_flag_violations) as quality_flag_violations,
        sum(service_observation_flag_violations) as service_observation_flag_violations,
        countif(row_count > 1) as duplicate_trip_violations
    from grouped_trips
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('trip_quality_accepted_values' as issue_type, trip_quality_violations as violation_count),
    struct('service_observation_class_accepted_values' as issue_type, service_observation_class_violations as violation_count),
    struct('quality_flags_accepted_values' as issue_type, quality_flag_violations as violation_count),
    struct('service_observation_flags_accepted_values' as issue_type, service_observation_flag_violations as violation_count),
    struct('unique_trip' as issue_type, duplicate_trip_violations as violation_count)
])
where violation_count > 0
