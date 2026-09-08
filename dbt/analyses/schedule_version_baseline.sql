-- Run ONLY while dim_schedule_version is still the frozen pre-migration table.
-- Both EXCEPT directions include every ID, validity bound, and lineage column.
with expected_only as (
    select * from {{ ref('dim_schedule_version') }}
    except distinct
    select * from {{ ref('int_schedule_version') }}
), actual_only as (
    select * from {{ ref('int_schedule_version') }}
    except distinct
    select * from {{ ref('dim_schedule_version') }}
)
select 'expected_only' as issue, to_json_string(t) as row_json from expected_only as t
union all
select 'actual_only' as issue, to_json_string(t) as row_json from actual_only as t
union all
select 'row_count_difference' as issue, cast(count(*) as string) as row_json
from {{ ref('int_schedule_version') }}
having count(*) != (select count(*) from {{ ref('dim_schedule_version') }})
