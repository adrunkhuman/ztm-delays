select service_id, service_date
from {{ ref('stg_gtfs_calendar_dates') }}
group by service_id, service_date
having count(*) > 1
