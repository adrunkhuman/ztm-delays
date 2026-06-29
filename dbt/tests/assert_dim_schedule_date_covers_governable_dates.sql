with loaded_snapshot_dates as (
    select date(snapshots.snapshot_timestamp, 'Europe/Warsaw') as snapshot_date
    from {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
    inner join (select distinct gtfs_snapshot_id from {{ ref('stg_gtfs__calendar_dates') }}) as loaded_snapshots
        on snapshots.snapshot_id = loaded_snapshots.gtfs_snapshot_id
),

first_governable_date as (
    select date_add(min(snapshot_date), interval 1 day) as service_date
    from loaded_snapshot_dates
)

select dim_date.service_date
from {{ ref('dim_date') }} as dim_date
cross join first_governable_date
left join {{ ref('dim_schedule_date') }} as dim_schedule_date
    on dim_date.service_date = dim_schedule_date.service_date
where dim_date.service_date >= first_governable_date.service_date
  and dim_schedule_date.service_date is null
