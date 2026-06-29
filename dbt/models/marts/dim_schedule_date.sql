with date_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date
    from {{ ref('stg_gtfs__calendar_dates') }}
),

date_spine as (
    select service_date
    from date_bounds,
        unnest(generate_date_array(min_service_date, max_service_date)) as service_date
),

loaded_snapshots as (
    select distinct gtfs_snapshot_id
    from {{ ref('stg_gtfs__calendar_dates') }}
),

governing_snapshots as (
    select
        date_spine.service_date,
        snapshots.snapshot_id as gtfs_snapshot_id,
        snapshots.snapshot_timestamp
    from date_spine
    inner join {{ source('raw', 'raw_gtfs_snapshots') }} as snapshots
        on date(snapshots.snapshot_timestamp, 'Europe/Warsaw') < date_spine.service_date
    inner join loaded_snapshots
        on snapshots.snapshot_id = loaded_snapshots.gtfs_snapshot_id
    qualify row_number() over (
        partition by date_spine.service_date
        order by snapshots.snapshot_timestamp desc, snapshots.snapshot_id desc
    ) = 1
),

calendar_dates as (
    select
        calendar_dates.service_id,
        governing_snapshots.service_date,
        governing_snapshots.gtfs_snapshot_id,
        -- Pc only means generic weekday; Warsaw service_id date prefixes carry the exact weekday pattern.
        safe_cast(regexp_extract(calendar_dates.service_id, r'^(\d{4}-\d{2}-\d{2}):') as date) as schedule_pattern_date,
        regexp_extract(calendar_dates.service_id, r'(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$') as schedule_token
    from governing_snapshots
    left join {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
        on governing_snapshots.gtfs_snapshot_id = calendar_dates.gtfs_snapshot_id
        and governing_snapshots.service_date = calendar_dates.service_date
),

calendar_schedule_classes as (
    select
        service_id,
        service_date,
        gtfs_snapshot_id,
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
        end as schedule_class
    from calendar_dates
),

schedule_classes as (
    select
        service_date,
        gtfs_snapshot_id,
        string_agg(distinct schedule_class, ', ' order by schedule_class) as schedule_day_types,
        coalesce(string_agg(distinct service_id, ', ' order by service_id), '') as schedule_service_ids,
        count(distinct if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class_count,
        max(if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class,
        count(distinct schedule_class) as schedule_class_count,
        max(schedule_class) as any_schedule_class
    from calendar_schedule_classes
    group by service_date, gtfs_snapshot_id
)

select
    service_date,
    case
        when specific_schedule_class_count = 1 then specific_schedule_class
        when specific_schedule_class_count > 1 then 'mixed'
        when schedule_class_count = 1 then any_schedule_class
        when schedule_class_count > 1 then 'mixed'
        else 'unknown'
    end as schedule_day_type,
    schedule_day_types,
    schedule_service_ids,
    gtfs_snapshot_id
from schedule_classes
