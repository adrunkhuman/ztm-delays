{{ config(tags=['audit']) }}

select
    left_range.line,
    left_range.direction_id,
    left_range.schedule_day_type,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('int_schedule_version') }} as left_range
inner join {{ ref('int_schedule_version') }} as right_range
    on left_range.line = right_range.line
    and left_range.direction_id = right_range.direction_id
    and left_range.schedule_day_type = right_range.schedule_day_type
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date
