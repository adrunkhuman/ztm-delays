select
    left_range.stop_id,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_stop_post') }} as left_range
inner join {{ ref('dim_stop_post') }} as right_range
    on left_range.stop_id = right_range.stop_id
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date
