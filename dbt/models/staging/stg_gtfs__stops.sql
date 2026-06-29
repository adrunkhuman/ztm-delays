select
    cast(stop_id as string) as stop_id,
    cast(stop_name as string) as stop_name,
    safe_cast(stop_lat as float64) as stop_lat,
    safe_cast(stop_lon as float64) as stop_lon,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_stops') }}
where safe_cast(stop_lat as float64) between 51.0 and 53.5
  and safe_cast(stop_lon as float64) between 19.5 and 22.5
