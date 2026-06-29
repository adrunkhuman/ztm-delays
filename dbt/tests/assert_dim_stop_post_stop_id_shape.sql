select
    stop_id,
    stop_group_id
from {{ ref('dim_stop_post') }}
where length(stop_id) < 4
   or stop_group_id != substr(stop_id, 1, 4)
