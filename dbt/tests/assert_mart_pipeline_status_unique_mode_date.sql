select
    service_date,
    mode,
    count(*) as row_count
from {{ ref('mart_pipeline_status') }}
where service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
    and date('{{ var("processing_date") }}')
group by service_date, mode
having count(*) > 1
