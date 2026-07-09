with date_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date,
        count(*) as date_count
    from {{ ref('dim_date') }}
)

select
    'dim_date_contiguous_spine' as issue_type,
    cast(min_service_date as string) as entity_id,
    min_service_date as issue_date,
    cast(date_count as string) as detail
from date_bounds
where date_count != date_diff(max_service_date, min_service_date, day) + 1

union all

select
    'dim_stop_post_stop_id_shape' as issue_type,
    cast(stop_id as string) as entity_id,
    cast(null as date) as issue_date,
    cast(stop_group_id as string) as detail
from {{ ref('dim_stop_post') }}
where length(stop_id) < 4
   or stop_group_id != substr(stop_id, 1, 4)

union all

select
    'dim_schedule_version_valid_range' as issue_type,
    cast(schedule_version_id as string) as entity_id,
    valid_from_date as issue_date,
    cast(valid_to_date as string) as detail
from {{ ref('dim_schedule_version') }}
where valid_to_date is not null
  and valid_to_date < valid_from_date

union all

select
    'dim_schedule_version_one_open_range' as issue_type,
    to_json_string(struct(line, direction_id, schedule_day_type)) as entity_id,
    cast(null as date) as issue_date,
    cast(count(*) as string) as detail
from {{ ref('dim_schedule_version') }}
where valid_to_date is null
group by line, direction_id, schedule_day_type
having count(*) > 1
