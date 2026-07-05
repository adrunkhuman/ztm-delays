select
    period_type,
    period_id,
    day_class_type,
    day_class,
    mode,
    line,
    route_short_name,
    direction_id,
    trip_headsign,
    schedule_version_id,
    hour_bracket
from {{ ref('agg_time_period') }}
where {{ period_partition_filter() }}
group by
    period_type,
    period_id,
    day_class_type,
    day_class,
    mode,
    line,
    route_short_name,
    direction_id,
    trip_headsign,
    schedule_version_id,
    hour_bracket
having count(*) > 1
