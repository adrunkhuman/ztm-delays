select
    vehicle_number,
    gps_time,
    count(*) as duplicate_count
from {{ ref('stg_gps_pings') }}
group by 1, 2
having count(*) > 1
