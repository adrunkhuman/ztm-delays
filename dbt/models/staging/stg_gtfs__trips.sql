select
    cast(trip_id as string) as trip_id,
    cast(route_id as string) as line,
    cast(service_id as string) as service_id,
    cast(trip_headsign as string) as trip_headsign,
    cast(direction_id as int64) as direction_id,
    coalesce(nullif(regexp_replace(cast(block_short_name as string), r'^0+', ''), ''), '0') as brigade,
    cast(shape_id as string) as shape_id,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_trips') }}
