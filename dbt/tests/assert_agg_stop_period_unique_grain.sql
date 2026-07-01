select
    period_type,
    period_id,
    day_class_type,
    day_class,
    schedule_version_id,
    stop_group_id,
    line,
    direction_id,
    trip_headsign,
    hour_bracket
from {{ ref('agg_stop_period') }}
group by
    period_type,
    period_id,
    day_class_type,
    day_class,
    schedule_version_id,
    stop_group_id,
    line,
    direction_id,
    trip_headsign,
    hour_bracket
having count(*) > 1
