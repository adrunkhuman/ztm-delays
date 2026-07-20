with grouped as (
    select
        processing_date,
        service_date,
        gtfs_snapshot_id,
        trip_id,
        count(*) as row_count
    from {{ ref('int_serving_trip_stop_profile') }}
    where processing_date = date('{{ var("processing_date") }}')
    group by processing_date, service_date, gtfs_snapshot_id, trip_id
)

select *
from grouped
where service_date is null
   or gtfs_snapshot_id is null
   or trip_id is null
   or row_count != 1
