select gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
from {{ ref('int_stop_arrivals') }}
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
having count(*) > 1
