with valid_snapshot as (
    select snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_timestamp <= timestamp(date('{{ var("processing_date") }}'), 'Europe/Warsaw')
    order by snapshot_timestamp desc, snapshot_id desc
    limit 1
)

select
    cast(route_id as string) as route_id,
    cast(route_short_name as string) as route_short_name,
    cast(route_type as int64) as route_type,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_routes') }}
inner join valid_snapshot
    on gtfs_snapshot_id = valid_snapshot.snapshot_id
