select model_name, period_type, day_class_type, hour_bracket, source_start_date, source_end_date
from (
    select
        'agg_line_stop_period' as model_name,
        period_type,
        day_class_type,
        hour_bracket,
        source_start_date,
        source_end_date
    from {{ ref('agg_line_stop_period') }}
    where {{ period_partition_filter() }}

    union all

    select
        'agg_stop_period' as model_name,
        period_type,
        day_class_type,
        hour_bracket,
        source_start_date,
        source_end_date
    from {{ ref('agg_stop_period') }}
    where {{ period_partition_filter() }}

    union all

    select
        'agg_time_period' as model_name,
        period_type,
        day_class_type,
        hour_bracket,
        source_start_date,
        source_end_date
    from {{ ref('agg_time_period') }}
    where {{ period_partition_filter() }}
)
where period_type not in ('month', 'schedule_version')
   or day_class_type not in ('day_type', 'weekday', 'schedule_day_type')
   or hour_bracket not between 0 and 23
   or source_start_date > source_end_date
