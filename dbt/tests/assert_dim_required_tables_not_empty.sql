select table_name
from (select 'dim_line' as table_name)
where not exists (select 1 from {{ ref('dim_line') }})

union all

select table_name
from (select 'dim_line_current' as table_name)
where not exists (select 1 from {{ ref('dim_line_current') }})

union all

select table_name
from (select 'dim_stop_post' as table_name)
where not exists (select 1 from {{ ref('dim_stop_post') }})

union all

select table_name
from (select 'dim_stop_post_current' as table_name)
where not exists (select 1 from {{ ref('dim_stop_post_current') }})

union all

select table_name
from (select 'dim_stop_group' as table_name)
where not exists (select 1 from {{ ref('dim_stop_group') }})

union all

select table_name
from (select 'dim_stop_group_current' as table_name)
where not exists (select 1 from {{ ref('dim_stop_group_current') }})

union all

select table_name
from (select 'dim_date' as table_name)
where not exists (select 1 from {{ ref('dim_date') }})

union all

select table_name
from (select 'dim_schedule_date' as table_name)
where not exists (select 1 from {{ ref('dim_schedule_date') }})

union all

select table_name
from (select 'dim_schedule_date_current' as table_name)
where not exists (select 1 from {{ ref('dim_schedule_date_current') }})

union all

select table_name
from (select 'dim_schedule_version' as table_name)
where not exists (select 1 from {{ ref('dim_schedule_version') }})
