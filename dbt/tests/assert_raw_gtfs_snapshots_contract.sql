{{ config(tags=['audit']) }}

with grouped_snapshots as (
    select
        snapshot_id,
        count(*) as row_count,
        countif(
            snapshot_id is null
            or snapshot_timestamp is null
            or file_hash is null
            or gcs_path is null
        ) as required_field_violations
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    group by snapshot_id
),

contract_counts as (
    select
        sum(required_field_violations) as required_field_violations,
        countif(row_count > 1) as duplicate_snapshot_id_violations
    from grouped_snapshots
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('required_fields_not_null' as issue_type, required_field_violations as violation_count),
    struct('unique_snapshot_id' as issue_type, duplicate_snapshot_id_violations as violation_count)
])
where violation_count > 0
