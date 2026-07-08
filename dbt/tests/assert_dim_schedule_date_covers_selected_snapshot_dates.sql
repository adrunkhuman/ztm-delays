with selected_snapshot_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date
    from {{ ref('stg_gtfs__calendar_dates') }}
    where gtfs_snapshot_id = '{{ var("gtfs_snapshot_id") }}'
),

selected_snapshot_dates as (
    select service_date
    from selected_snapshot_bounds,
        unnest(generate_date_array(min_service_date, max_service_date)) as service_date
)

select selected_snapshot_dates.service_date
from selected_snapshot_dates
left join {{ ref('dim_schedule_date') }} as dim_schedule_date
    on selected_snapshot_dates.service_date = dim_schedule_date.service_date
    and dim_schedule_date.gtfs_snapshot_id = '{{ var("gtfs_snapshot_id") }}'
where dim_schedule_date.service_date is null
