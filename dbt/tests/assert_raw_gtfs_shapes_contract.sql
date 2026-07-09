{{ config(tags=['audit']) }}

with contract_counts as (
    select
        countif(
            raw_shapes.shape_id is null
            or raw_shapes.shape_pt_sequence is null
            or raw_shapes.gtfs_snapshot_id is null
        ) as required_field_violations,
        countif(snapshots.snapshot_id is null) as snapshot_relationship_violations
    from {{ source('raw', 'raw_gtfs_shapes') }} as raw_shapes
    left join {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
        on raw_shapes.gtfs_snapshot_id = snapshots.snapshot_id
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('gtfs_snapshot_relationship' as issue_type, snapshot_relationship_violations as violation_count)
])
where violation_count > 0
