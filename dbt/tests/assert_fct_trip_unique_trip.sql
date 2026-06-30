select gtfs_snapshot_id, service_date, trip_id, vehicle_number
from {{ ref('fct_trip') }}
where service_date = date('{{ var("processing_date") }}')
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number
having count(*) > 1
