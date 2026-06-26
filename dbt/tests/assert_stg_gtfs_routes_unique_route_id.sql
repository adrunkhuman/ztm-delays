select route_id
from {{ ref('stg_gtfs_routes') }}
group by route_id
having count(*) > 1
