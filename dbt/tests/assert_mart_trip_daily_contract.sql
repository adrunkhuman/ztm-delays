select
    service_date,
    trip_id,
    vehicle_number
from {{ ref('mart_trip_daily') }}
where service_date = date('{{ var("processing_date", "1970-01-01") }}')
  and (
      service_date is null
      or trip_id is null
      or vehicle_number is null
  )

union all

select
    service_date,
    trip_id,
    vehicle_number
from {{ ref('mart_trip_daily') }}
where service_date = date('{{ var("processing_date", "1970-01-01") }}')
group by service_date, trip_id, vehicle_number
having count(*) > 1
