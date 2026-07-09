{{ config(tags=['audit']) }}

with contract_counts as (
    select
        countif(
            stop_times.trip_id is null
            or stop_times.stop_id is null
            or stop_times.stop_sequence is null
            or stop_times.arrival_time is null
            or stop_times.departure_time is null
            or stop_times.gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(snapshots.snapshot_id is null) as snapshot_relationship_violations
    from {{ source('raw', 'raw_gtfs_stop_times') }} as stop_times
    left join {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
        on stop_times.gtfs_snapshot_id = snapshots.snapshot_id
    where stop_times.gtfs_snapshot_id = '{{ var("gtfs_snapshot_id", "__missing_snapshot__") }}'
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('gtfs_snapshot_relationship' as issue_type, snapshot_relationship_violations as violation_count)
])
where violation_count > 0
