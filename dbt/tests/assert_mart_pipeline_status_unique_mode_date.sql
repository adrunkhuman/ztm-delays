select
    service_date,
    mode,
    count(*) as row_count
from {{ ref('mart_pipeline_status') }}
group by service_date, mode
having count(*) > 1
