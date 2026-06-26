select vehicle_number, gps_time
from {{ ref('int_ping_trip') }}
group by vehicle_number, gps_time
having count(*) > 1
