with expected as (
    select
        gps_date,
        mode
    from unnest([date('{{ var("processing_date") }}')]) as gps_date
    cross join unnest(['bus', 'tram']) as mode
),

actual as (
    select
        gps_date,
        mode
    from {{ ref('mart_day_completeness') }}
    where gps_date = date('{{ var("processing_date") }}')
),

duplicate_rows as (
    select
        'duplicate' as issue_type,
        gps_date,
        mode,
        count(*) as row_count
    from actual
    group by gps_date, mode
    having count(*) > 1
),

missing_rows as (
    select
        'missing' as issue_type,
        expected.gps_date,
        expected.mode,
        0 as row_count
    from expected
    left join actual
        on expected.gps_date = actual.gps_date
        and expected.mode = actual.mode
    where actual.gps_date is null
)

select * from duplicate_rows
union all
select * from missing_rows
