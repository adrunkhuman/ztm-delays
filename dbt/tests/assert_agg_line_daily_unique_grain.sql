select
    service_date,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign
from {{ ref('agg_line_daily') }}
group by
    service_date,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign
having count(*) > 1
