select gtfs_snapshot_id, service_date, trip_id, vehicle_number
from {{ ref('fct_trip') }}
where service_date between date_sub(date('{{ var("processing_date", "1970-01-01") }}'), interval 1 day)
    and date('{{ var("processing_date", "1970-01-01") }}')
group by gtfs_snapshot_id, service_date, trip_id, vehicle_number
having count(*) > 1
