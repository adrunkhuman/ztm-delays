select trip_id
from {{ ref('stg_gtfs_trips') }}
group by trip_id
having count(*) > 1
