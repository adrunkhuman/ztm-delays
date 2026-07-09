select gtfs_snapshot_id, processing_date, service_date, trip_id
from {{ ref('int_gtfs_trip_schedule') }}
where processing_date = date('{{ var("processing_date", "1970-01-01") }}')
group by gtfs_snapshot_id, processing_date, service_date, trip_id
having count(*) > 1
