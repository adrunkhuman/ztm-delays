select gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
from {{ ref('fct_stop_arrival') }}
where service_date = date('{{ var("processing_date") }}')
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
having count(*) > 1
