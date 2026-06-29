with selected_snapshot as (
    select snapshot_id as gtfs_snapshot_id
    from {{ source('raw', 'raw_gtfs_snapshots') }}
    where snapshot_id = '{{ var("gtfs_snapshot_id") }}'
    limit 1
),

calendar_dates as (
    select
        calendar_dates.service_id,
        calendar_dates.service_date,
        calendar_dates.gtfs_snapshot_id,
        safe_cast(regexp_extract(calendar_dates.service_id, r'^(\d{4}-\d{2}-\d{2}):') as date) as schedule_pattern_date,
        regexp_extract(calendar_dates.service_id, r'(?:^|:)(Pc|Pt|Sb|Nd)[A-Za-z]*$') as schedule_token
    from {{ ref('stg_gtfs__calendar_dates') }} as calendar_dates
    inner join selected_snapshot
        on calendar_dates.gtfs_snapshot_id = selected_snapshot.gtfs_snapshot_id
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

date_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date
    from calendar_dates
),

date_spine as (
    select service_date
    from date_bounds,
        unnest(generate_date_array(min_service_date, max_service_date)) as service_date
),

schedule_classes as (
    select
        service_date,
        gtfs_snapshot_id,
        string_agg(distinct schedule_class, ', ' order by schedule_class) as schedule_day_types,
        string_agg(distinct service_id, ', ' order by service_id) as schedule_service_ids,
        count(distinct if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class_count,
        max(if(schedule_class != 'weekday', schedule_class, null)) as specific_schedule_class,
        count(distinct schedule_class) as schedule_class_count,
        max(schedule_class) as any_schedule_class
    from calendar_schedule_classes
    group by service_date, gtfs_snapshot_id
),

years as (
    select distinct extract(year from service_date) as year
    from date_spine
),

easter_terms as (
    select
        year,
        mod(year, 19) as a,
        div(year, 100) as b,
        mod(year, 100) as c
    from years
),

easter_terms_2 as (
    select
        year,
        a,
        b,
        c,
        div(b, 4) as d,
        mod(b, 4) as e,
        div(b + 8, 25) as f,
        div(b - div(b + 8, 25) + 1, 3) as g,
        div(c, 4) as i,
        mod(c, 4) as k
    from easter_terms
),

easter_terms_3 as (
    select
        year,
        a,
        b,
        c,
        e,
        i,
        k,
        mod(19 * a + b - d - g + 15, 30) as h
    from easter_terms_2
),

easter_dates as (
    select
        year,
        date(
            year,
            div(h + mod(32 + 2 * e + 2 * i - h - k, 7) - 7 * div(a + 11 * h + 22 * mod(32 + 2 * e + 2 * i - h - k, 7), 451) + 114, 31),
            mod(h + mod(32 + 2 * e + 2 * i - h - k, 7) - 7 * div(a + 11 * h + 22 * mod(32 + 2 * e + 2 * i - h - k, 7), 451) + 114, 31) + 1
        ) as easter_sunday
    from easter_terms_3
),

holidays as (
    select date(year, 1, 1) as holiday_date from years
    union all select date(year, 1, 6) from years
    union all select date(year, 5, 1) from years
    union all select date(year, 5, 3) from years
    union all select date(year, 8, 15) from years
    union all select date(year, 11, 1) from years
    union all select date(year, 11, 11) from years
    union all select date(year, 12, 25) from years
    union all select date(year, 12, 26) from years
    union all select easter_sunday from easter_dates
    union all select date_add(easter_sunday, interval 1 day) from easter_dates
    union all select date_add(easter_sunday, interval 49 day) from easter_dates
    union all select date_add(easter_sunday, interval 60 day) from easter_dates
)

select
    date_spine.service_date,
    case
        when extract(dayofweek from date_spine.service_date) in (1, 7) then 'weekend'
        else 'weekday'
    end as day_type,
    date_spine.service_date in (select holiday_date from holidays) as is_holiday,
    format_date('%A', date_spine.service_date) as weekday_name,
    extract(dayofweek from date_spine.service_date) as day_of_week,
    extract(isoweek from date_spine.service_date) as iso_week,
    extract(month from date_spine.service_date) as month,
    extract(year from date_spine.service_date) as year,
    case
        when schedule_classes.specific_schedule_class_count = 1 then schedule_classes.specific_schedule_class
        when schedule_classes.specific_schedule_class_count > 1 then 'mixed'
        when schedule_classes.schedule_class_count = 1 then schedule_classes.any_schedule_class
        when schedule_classes.schedule_class_count > 1 then 'mixed'
        else 'unknown'
    end as schedule_day_type,
    schedule_classes.schedule_day_types,
    schedule_classes.schedule_service_ids,
    selected_snapshot.gtfs_snapshot_id
from date_spine
cross join selected_snapshot
left join schedule_classes
    on date_spine.service_date = schedule_classes.service_date
    and selected_snapshot.gtfs_snapshot_id = schedule_classes.gtfs_snapshot_id
