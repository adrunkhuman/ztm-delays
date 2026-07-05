select
    service_date,
    scheduled_start_date,
    gtfs_snapshot_id,
    line,
    direction_id,
    trip_headsign,
    schedule_day_type,
    schedule_version_id,
    service_hour,
    count(*) as row_count
from {{ ref('agg_service_coverage') }}
where scheduled_start_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
group by
    service_date,
    scheduled_start_date,
    gtfs_snapshot_id,
    line,
    direction_id,
    trip_headsign,
    schedule_day_type,
    schedule_version_id,
    service_hour
having count(*) > 1
