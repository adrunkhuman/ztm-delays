select
    stop_group_id,
    valid_from_date,
    count(*) as row_count
from {{ ref('dim_stop_group') }}
group by stop_group_id, valid_from_date
having row_count > 1
