select gtfs_snapshot_id, processing_date, service_date, trip_id
from {{ ref('int_gtfs_trip_schedule') }}
group by gtfs_snapshot_id, processing_date, service_date, trip_id
having count(*) > 1
