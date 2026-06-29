select line, direction_id, schedule_day_type
from {{ ref('dim_schedule_version') }}
where valid_to_date is null
group by line, direction_id, schedule_day_type
having count(*) > 1
