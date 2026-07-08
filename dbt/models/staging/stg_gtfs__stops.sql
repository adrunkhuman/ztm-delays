select
    cast(stop_id as string) as stop_id,
    cast(stop_name as string) as stop_name,
    cast(stop_code as string) as stop_code,
    safe_cast(stop_lat as float64) as stop_lat,
    safe_cast(stop_lon as float64) as stop_lon,
    cast(zone_id as string) as zone_id,
    case
        when cast(zone_id as string) = '1+2' then '1'
        else cast(zone_id as string)
    end as effective_zone_id,
    cast(stop_name_stem as string) as stop_name_stem,
    cast(town_name as string) as town_name,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_stops') }}
where safe_cast(stop_lat as float64) between 51.0 and 53.5
  and safe_cast(stop_lon as float64) between 19.5 and 22.5
