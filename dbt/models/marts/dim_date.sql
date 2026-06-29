with service_date_bounds as (
    select
        min(service_date) as min_service_date,
        max(service_date) as max_service_date
    from {{ ref('stg_gtfs__calendar_dates') }}
),

date_spine as (
    select service_date
    from service_date_bounds,
        unnest(generate_date_array(min_service_date, max_service_date)) as service_date
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
    service_date,
    case
        when extract(dayofweek from service_date) in (1, 7) then 'weekend'
        else 'weekday'
    end as day_type,
    service_date in (select holiday_date from holidays) as is_holiday,
    format_date('%A', service_date) as weekday_name,
    extract(dayofweek from service_date) as day_of_week,
    extract(isoweek from service_date) as iso_week,
    extract(month from service_date) as month,
    extract(year from service_date) as year
from date_spine
