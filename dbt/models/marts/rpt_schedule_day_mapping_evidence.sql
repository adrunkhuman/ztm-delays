{{ config(materialized='table', tags=['audit']) }}

with service_tokens as (
    select
        service_id,
        service_date,
        gtfs_snapshot_id,
        safe_cast(regexp_extract(service_id, r'^(\d{4}-\d{2}-\d{2}):') as date) as schedule_pattern_date,
        regexp_extract(service_id, r'(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$') as schedule_token
    from {{ ref('stg_gtfs__calendar_dates') }}
),

classified as (
    select
        service_tokens.*,
        case
            when schedule_token = 'Nd' then 'sunday_holiday'
            when schedule_token = 'Sb' then 'saturday'
            when schedule_token = 'Pt' then 'friday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 2 then 'monday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 3 then 'tuesday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 4 then 'wednesday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 5 then 'thursday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 6 then 'friday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 7 then 'saturday'
            when schedule_pattern_date is not null and schedule_token = 'Pc'
                and extract(dayofweek from schedule_pattern_date) = 1 then 'sunday_holiday'
            when schedule_token = 'Pc' then 'weekday'
            else 'unknown'
        end as token_schedule_day_type
    from service_tokens
)

select
    token_schedule_day_type,
    schedule_token,
    count(*) as service_date_count,
    count(distinct service_id) as service_id_count,
    min(service_date) as first_service_date,
    max(service_date) as last_service_date,
    array_agg(distinct service_id order by service_id limit 20) as example_service_ids
from classified
group by token_schedule_day_type, schedule_token
