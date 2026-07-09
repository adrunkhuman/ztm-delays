{{ config(tags=['audit']) }}

with grouped_shapes as (
    select
        gtfs_snapshot_id,
        shape_id,
        shape_pt_sequence,
        count(*) as row_count,
        countif(
            shape_id is null
            or shape_pt_lat is null
            or shape_pt_lon is null
            or shape_pt_sequence is null
            or gtfs_snapshot_id is null
        ) as required_field_violations
    from {{ ref('stg_gtfs__shapes') }}
    group by gtfs_snapshot_id, shape_id, shape_pt_sequence
),

contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        countif(row_count > 1) as duplicate_shape_sequence_violations
    from grouped_shapes
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('unique_shape_sequence' as issue_type, duplicate_shape_sequence_violations as violation_count)
])
where violation_count > 0
