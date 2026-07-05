select
    service_date,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign
from {{ ref('agg_line_daily') }}
where service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
group by
    service_date,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign
having count(*) > 1
