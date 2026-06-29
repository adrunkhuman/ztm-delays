select
    left_range.line,
    left_range.valid_from_date as left_valid_from_date,
    left_range.valid_to_date as left_valid_to_date,
    right_range.valid_from_date as right_valid_from_date,
    right_range.valid_to_date as right_valid_to_date
from {{ ref('dim_line') }} as left_range
inner join {{ ref('dim_line') }} as right_range
    on left_range.line = right_range.line
    and left_range.valid_from_date < right_range.valid_from_date
    and coalesce(left_range.valid_to_date, date '9999-12-31') >= right_range.valid_from_date
