select table_name
from (select 'dim_schedule_version' as table_name)
where not exists (select 1 from {{ ref('dim_schedule_version') }})
