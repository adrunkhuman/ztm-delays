{{ config(tags=['audit']) }}

with contract_counts as (
    select
        countif(
            schedule_version_id is null
            or line is null
            or direction_id is null
            or schedule_day_type is null
            or timetable_fingerprint is null
            or valid_from_date is null
            or first_gtfs_snapshot_id is null
            or last_gtfs_snapshot_id is null
            or first_processing_date is null
            or last_processing_date is null
            or max_scheduled_trip_count is null
        ) as required_field_violations,
        countif(schedule_day_type not in (
            'monday',
            'tuesday',
            'wednesday',
            'thursday',
            'friday',
            'saturday',
            'sunday_holiday',
            'weekday',
            'mixed',
            'unknown'
        )) as schedule_day_type_violations,
        countif(direction_id not in (0, 1)) as direction_id_violations
    from {{ ref('int_schedule_version') }}
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('schedule_day_type_accepted_values' as issue_type, schedule_day_type_violations as violation_count),
    struct('direction_id_accepted_values' as issue_type, direction_id_violations as violation_count)
])
where violation_count > 0
