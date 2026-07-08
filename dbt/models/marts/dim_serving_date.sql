{{ config(materialized='table') }}

with dates as (
    select distinct service_date
    from {{ ref('mart_mode_window_summary') }}
    where window_type = 'day'
      and source_end_date >= date '1900-01-01'
)

select
    service_date,
    cast(service_date as string) as service_date_key,
    lag(service_date) over (order by service_date) as previous_service_date,
    lead(service_date) over (order by service_date) as next_service_date,
    service_date = max(service_date) over () as is_latest,
    row_number() over (order by service_date desc) as service_date_rank_desc
from dates
