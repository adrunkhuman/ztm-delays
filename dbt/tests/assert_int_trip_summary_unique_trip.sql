select gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
from {{ ref('int_trip_summary') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by gtfs_snapshot_id, gps_date, service_date, trip_id, vehicle_number
having count(*) > 1
