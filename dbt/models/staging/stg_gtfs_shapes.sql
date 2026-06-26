with valid_snapshot as (
    select snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
)

select
    cast(shape_id as string) as shape_id,
    cast(shape_pt_lat as float64) as shape_pt_lat,
    cast(shape_pt_lon as float64) as shape_pt_lon,
    cast(shape_pt_sequence as int64) as shape_pt_sequence,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_shapes') }}
inner join valid_snapshot
    on gtfs_snapshot_id = valid_snapshot.snapshot_id
