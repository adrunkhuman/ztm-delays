select stop_id
from {{ ref('stg_gtfs_stops') }}
group by stop_id
having count(*) > 1
