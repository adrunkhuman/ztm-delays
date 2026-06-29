select
    gps_date,
    gps_hour,
    vehicle_type,
    count(*) as row_count
from {{ ref('int_gps_hourly_completeness') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by gps_date, gps_hour, vehicle_type
having count(*) > 1
