select model_name, period_type, period_id, n, histogram_n
from (
    select
        'agg_line_stop_period' as model_name,
        period_type,
        period_id,
        n,
        (select sum(bucket.n) from unnest(delay_histogram) as bucket) as histogram_n
    from {{ ref('agg_line_stop_period') }}

    union all

    select
        'agg_stop_period' as model_name,
        period_type,
        period_id,
        n,
        (select sum(bucket.n) from unnest(delay_histogram) as bucket) as histogram_n
    from {{ ref('agg_stop_period') }}

    union all

    select
        'agg_time_period' as model_name,
        period_type,
        period_id,
        n,
        (select sum(bucket.n) from unnest(delay_histogram) as bucket) as histogram_n
    from {{ ref('agg_time_period') }}

    union all

    select
        'agg_line_daily' as model_name,
        'day' as period_type,
        cast(service_date as string) as period_id,
        n,
        (select sum(bucket.n) from unnest(delay_histogram) as bucket) as histogram_n
    from {{ ref('agg_line_daily') }}
    where service_date between date('{{ var("aggregation_start_date", var("processing_date")) }}')
        and date('{{ var("processing_date") }}')
)
where n != histogram_n
