select gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
from {{ ref('int_stop_arrivals') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
having count(*) > 1
