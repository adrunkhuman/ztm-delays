select table_name
from (select 'dim_date' as table_name)
where not exists (select 1 from {{ ref('dim_date') }})
