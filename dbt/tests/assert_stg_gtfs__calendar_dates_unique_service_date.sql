select gtfs_snapshot_id, service_id, service_date
from {{ ref('stg_gtfs__calendar_dates') }}
group by gtfs_snapshot_id, service_id, service_date
having count(*) > 1
