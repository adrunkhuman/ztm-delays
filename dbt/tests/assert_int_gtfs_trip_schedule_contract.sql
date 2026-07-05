with contract_counts as (
    select
        countif(
            service_date is null
            or processing_date is null
            or schedule_day_type is null
            or schedule_service_ids is null
            or gtfs_snapshot_id is null
            or line is null
            or direction_id is null
            or trip_id is null
            or service_id is null
            or shape_id is null
            or trip_start_seconds is null
            or trip_end_seconds is null
            or stop_count is null
            or ordered_stop_ids is null
            or ordered_arrival_time_seconds is null
            or trip_timetable_signature is null
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
    from {{ ref('int_gtfs_trip_schedule') }}
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('schedule_day_type_accepted_values' as issue_type, schedule_day_type_violations as violation_count),
    struct('direction_id_accepted_values' as issue_type, direction_id_violations as violation_count)
])
where violation_count > 0
