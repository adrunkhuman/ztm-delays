select *
from {{ ref('int_ping_trip') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
    line is null
    or brigade is null
    or gps_time is null
    or vehicle_number is null
    or trip_id is null
    or shape_id is null
    or gtfs_snapshot_id is null
    or service_id is null
    or direction_id is null
    or service_date is null
    or day_type is null
    or trip_start_seconds is null
    or trip_end_seconds is null
    or gps_time_seconds is null
  )
