select line, direction_id, schedule_day_type, valid_from_date
from {{ ref('dim_schedule_version') }}
group by line, direction_id, schedule_day_type, valid_from_date
having count(*) > 1
