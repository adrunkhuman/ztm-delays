select
    line,
    valid_from_date,
    count(*) as row_count
from {{ ref('dim_line') }}
group by line, valid_from_date
having row_count > 1
