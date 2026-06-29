select gtfs_snapshot_id, stop_id
from {{ ref('stg_gtfs__stops') }}
group by gtfs_snapshot_id, stop_id
having count(*) > 1
