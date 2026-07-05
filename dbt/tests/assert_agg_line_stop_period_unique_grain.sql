select
    period_type,
    period_id,
    day_class_type,
    day_class,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    stop_group_name,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    hour_bracket
from {{ ref('agg_line_stop_period') }}
where {{ period_partition_filter() }}
group by
    period_type,
    period_id,
    day_class_type,
    day_class,
    schedule_version_id,
    line,
    route_short_name,
    mode,
    direction_id,
    trip_headsign,
    stop_group_name,
    stop_id,
    stop_name,
    stop_lat,
    stop_lon,
    hour_bracket
having count(*) > 1
