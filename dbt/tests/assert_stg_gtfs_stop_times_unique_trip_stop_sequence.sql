select trip_id, stop_sequence
from {{ ref('stg_gtfs_stop_times') }}
group by trip_id, stop_sequence
having count(*) > 1
