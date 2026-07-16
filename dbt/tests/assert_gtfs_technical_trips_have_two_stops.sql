select
    service_date,
    processing_date,
    gtfs_snapshot_id,
    trip_id,
    stop_count
from {{ ref('int_gtfs_trip_schedule') }}
where processing_date = date('{{ var("processing_date") }}')
  and not is_public_service_segment
  and stop_count != 2
