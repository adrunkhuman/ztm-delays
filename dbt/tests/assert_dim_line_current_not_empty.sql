select table_name
from (select 'dim_line_current' as table_name)
where not exists (select 1 from {{ ref('dim_line_current') }})
