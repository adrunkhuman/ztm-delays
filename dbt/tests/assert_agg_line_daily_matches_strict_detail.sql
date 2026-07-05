with expected as (
    select
        arrivals.service_date,
        arrivals.schedule_version_id,
        arrivals.line,
        arrivals.route_short_name,
        arrivals.mode,
        arrivals.direction_id,
        arrivals.trip_headsign,
        count(*) as n
    from {{ ref('fct_stop_arrival') }} as arrivals
    where arrivals.service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
        and date('{{ var("processing_date") }}')
      and arrivals.trip_quality = 'complete'
    group by
        arrivals.service_date,
        arrivals.schedule_version_id,
        arrivals.line,
        arrivals.route_short_name,
        arrivals.mode,
        arrivals.direction_id,
        arrivals.trip_headsign
),

actual as (
    select
        service_date,
        schedule_version_id,
        line,
        route_short_name,
        mode,
        direction_id,
        trip_headsign,
        n
    from {{ ref('agg_line_daily') }}
    where service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
        and date('{{ var("processing_date") }}')
)

(select * from expected
except distinct
select * from actual)

union all

(select * from actual
except distinct
select * from expected)
