select gtfs_snapshot_id, trip_id, stop_sequence
from {{ ref('stg_gtfs__stop_times') }}
group by gtfs_snapshot_id, trip_id, stop_sequence
having count(*) > 1
