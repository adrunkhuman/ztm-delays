select table_name
from (select 'dim_stop_group' as table_name)
where not exists (select 1 from {{ ref('dim_stop_group') }})
