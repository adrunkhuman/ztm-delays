select
    'dim_line' as table_name,
    cast(left_range.line as string) as entity_id,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_line') }} as left_range
inner join {{ ref('dim_line') }} as right_range
    on left_range.line = right_range.line
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date

union all

select
    'dim_stop_post' as table_name,
    cast(left_range.stop_id as string) as entity_id,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_stop_post') }} as left_range
inner join {{ ref('dim_stop_post') }} as right_range
    on left_range.stop_id = right_range.stop_id
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date

union all

select
    'dim_stop_group' as table_name,
    cast(left_range.stop_group_id as string) as entity_id,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_stop_group') }} as left_range
inner join {{ ref('dim_stop_group') }} as right_range
    on left_range.stop_group_id = right_range.stop_group_id
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date

union all

select
    'dim_schedule_version' as table_name,
    to_json_string(struct(left_range.line, left_range.direction_id, left_range.schedule_day_type)) as entity_id,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_schedule_version') }} as left_range
inner join {{ ref('dim_schedule_version') }} as right_range
    on left_range.line = right_range.line
    and left_range.direction_id = right_range.direction_id
    and left_range.schedule_day_type = right_range.schedule_day_type
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date
