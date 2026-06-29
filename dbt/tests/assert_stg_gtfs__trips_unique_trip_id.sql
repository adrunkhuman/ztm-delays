select gtfs_snapshot_id, trip_id
from {{ ref('stg_gtfs__trips') }}
group by gtfs_snapshot_id, trip_id
having count(*) > 1
