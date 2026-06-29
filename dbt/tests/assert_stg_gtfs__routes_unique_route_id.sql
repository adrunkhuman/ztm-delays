select gtfs_snapshot_id, route_id
from {{ ref('stg_gtfs__routes') }}
group by gtfs_snapshot_id, route_id
having count(*) > 1
