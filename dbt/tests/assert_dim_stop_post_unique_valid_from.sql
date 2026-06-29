select
    stop_id,
    valid_from_date,
    count(*) as row_count
from {{ ref('dim_stop_post') }}
group by stop_id, valid_from_date
having row_count > 1
