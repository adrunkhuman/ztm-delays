select
    cast(shape_id as string) as shape_id,
    safe_cast(shape_pt_lat as float64) as shape_pt_lat,
    safe_cast(shape_pt_lon as float64) as shape_pt_lon,
    cast(shape_pt_sequence as int64) as shape_pt_sequence,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_shapes') }}
where safe_cast(shape_pt_lat as float64) between 51.0 and 53.5
  and safe_cast(shape_pt_lon as float64) between 19.5 and 22.5
