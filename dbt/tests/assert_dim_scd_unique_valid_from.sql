select 'dim_line' as table_name, cast(line as string) as entity_id, valid_from_date, count(*) as row_count
from {{ ref('dim_line') }}
group by line, valid_from_date
having row_count > 1

union all

select 'dim_stop_post' as table_name, cast(stop_id as string) as entity_id, valid_from_date, count(*) as row_count
from {{ ref('dim_stop_post') }}
group by stop_id, valid_from_date
having row_count > 1

union all

select 'dim_stop_group' as table_name, cast(stop_group_id as string) as entity_id, valid_from_date, count(*) as row_count
from {{ ref('dim_stop_group') }}
group by stop_group_id, valid_from_date
having row_count > 1

union all

select
    'dim_schedule_version' as table_name,
    to_json_string(struct(line, direction_id, schedule_day_type)) as entity_id,
    valid_from_date,
    count(*) as row_count
from {{ ref('dim_schedule_version') }}
group by line, direction_id, schedule_day_type, valid_from_date
having row_count > 1
