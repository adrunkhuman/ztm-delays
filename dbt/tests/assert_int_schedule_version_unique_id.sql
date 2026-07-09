{{ config(tags=['audit']) }}

select schedule_version_id, count(*) as row_count
from {{ ref('int_schedule_version') }}
group by schedule_version_id
having count(*) > 1
