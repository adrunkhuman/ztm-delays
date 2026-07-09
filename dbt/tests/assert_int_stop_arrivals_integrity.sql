select
    'detection_method_valid' as issue_type,
    cast(null as string) as gtfs_snapshot_id,
    cast(null as date) as service_date,
    cast(null as string) as trip_id,
    cast(null as string) as vehicle_number,
    cast(null as int64) as stop_sequence,
    cast(detection_method as string) as detail
from {{ ref('int_stop_arrivals') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
  and detection_method not in ('segment_within_75m', 'segment_within_250m')

union all

select
    'unique_trip_stop' as issue_type,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id,
    service_date,
    trip_id,
    vehicle_number,
    stop_sequence,
    cast(count(*) as string) as detail
from {{ ref('int_stop_arrivals') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number, stop_sequence
having count(*) > 1
