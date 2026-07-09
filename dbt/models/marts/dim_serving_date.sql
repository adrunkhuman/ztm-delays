{{ config(materialized='table') }}

with dates as (
    select distinct source_end_date as service_date
    from {{ ref('mart_mode_window_summary') }}
    where window_type = 'day'
      and source_end_date >= date '1900-01-01'
),

complete_dates as (
    select service_date
    from {{ ref('mart_pipeline_status') }}
    where mode in ('bus', 'tram')
      and service_date >= date '1900-01-01'
    group by service_date
    having count(distinct mode) = 2
       and logical_and(is_complete_day)
),

latest_complete_date as (
    select max(service_date) as service_date
    from complete_dates
)

select
    service_date,
    cast(service_date as string) as service_date_key,
    lag(service_date) over (order by service_date) as previous_service_date,
    lead(service_date) over (order by service_date) as next_service_date,
    service_date = (select service_date from latest_complete_date) as is_latest,
    row_number() over (order by service_date desc) as service_date_rank_desc
from dates
