with valid_snapshot as (
    select snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
)

select
    cast(stop_id as string) as stop_id,
    cast(stop_name as string) as stop_name,
    cast(stop_lat as float64) as stop_lat,
    cast(stop_lon as float64) as stop_lon,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_stops') }}
inner join valid_snapshot
    on gtfs_snapshot_id = valid_snapshot.snapshot_id
