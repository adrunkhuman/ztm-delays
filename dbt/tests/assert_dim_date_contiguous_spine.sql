with bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date,
        count(*) as date_count
    from {{ ref('dim_date') }}
)

select *
from bounds
where date_count != date_diff(max_service_date, min_service_date, day) + 1
