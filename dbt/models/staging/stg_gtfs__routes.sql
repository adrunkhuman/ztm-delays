select
    cast(route_id as string) as route_id,
    cast(route_short_name as string) as route_short_name,
    cast(route_type as int64) as route_type,
    case cast(route_type as int64)
        when 0 then 'tram'
        when 1 then 'metro'
        when 2 then 'rail'
        when 3 then 'bus'
        else 'unknown'
    end as mode,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_routes') }}
