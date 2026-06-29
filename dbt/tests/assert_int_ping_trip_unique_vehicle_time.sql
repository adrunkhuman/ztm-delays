select vehicle_number, gps_time
from {{ ref('int_ping_trip') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by vehicle_number, gps_time
having count(*) > 1
