with expected as (
    select
        gps_date,
        mode
    from unnest(generate_date_array(
        date('{{ var("aggregation_start_date", var("processing_date")) }}'),
        date('{{ var("processing_date") }}')
    )) as gps_date
    cross join unnest(['bus', 'tram']) as mode
),

duplicate_rows as (
    select
        'duplicate' as issue_type,
        gps_date,
        mode,
        count(*) as row_count
    from {{ ref('mart_day_completeness') }}
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
    left join {{ ref('mart_day_completeness') }} as actual
        on expected.gps_date = actual.gps_date
        and expected.mode = actual.mode
    where actual.gps_date is null
)

select * from duplicate_rows
union all
select * from missing_rows
