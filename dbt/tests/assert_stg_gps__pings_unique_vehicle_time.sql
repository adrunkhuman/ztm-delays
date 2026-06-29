select
    vehicle_number,
    gps_time,
    count(*) as duplicate_count
from {{ ref('stg_gps__pings') }}
where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
group by 1, 2
having count(*) > 1
