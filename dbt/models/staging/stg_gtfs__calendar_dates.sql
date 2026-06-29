select
    cast(service_id as string) as service_id,
    cast(date as date) as service_date,
    cast(exception_type as int64) as exception_type,
    case
        when extract(dayofweek from cast(date as date)) in (1, 7) then 'weekend'
        else 'weekday'
    end as day_type,
    cast(gtfs_snapshot_id as string) as gtfs_snapshot_id
from {{ source('raw', 'raw_gtfs_calendar_dates') }}
where cast(exception_type as int64) = 1
