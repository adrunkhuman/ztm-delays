{{ config(tags=['audit']) }}

with contract_counts as (
    select
        countif(
            calendar_dates.service_id is null
            or calendar_dates.date is null
            or calendar_dates.exception_type is null
            or calendar_dates.gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(calendar_dates.exception_type not in (1, 2)) as exception_type_violations,
        countif(snapshots.snapshot_id is null) as snapshot_relationship_violations
    from {{ source('raw', 'raw_gtfs_calendar_dates') }} as calendar_dates
    left join {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
        on calendar_dates.gtfs_snapshot_id = snapshots.snapshot_id
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('exception_type_accepted_values' as issue_type, exception_type_violations as violation_count),
    struct('gtfs_snapshot_relationship' as issue_type, snapshot_relationship_violations as violation_count)
])
where violation_count > 0
