{% set processing_date = var("processing_date", "1970-01-01") %}
{% set lookback_days = var("serving_window_lookback_days", 420) %}
{% set matching_date_count = var("serving_window_matching_date_count", 60) %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "source_end_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        cluster_by=["window_type", "service_date"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

with observed_dates as (
    select
        service_date,
        any_value(schedule_day_type) as schedule_day_type
    from {{ ref('int_serving_observed_date') }}
    where service_date between date_sub(date('{{ processing_date }}'), interval {{ lookback_days }} day)
        and date('{{ processing_date }}')
    group by service_date
),

eligible_dates as (
    select
        service_date,
        schedule_day_type,
        case
            when schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday') then 'weekdays'
            when schedule_day_type in ('saturday', 'sunday_holiday') then 'weekend'
        end as schedule_window_type
    from observed_dates
),

ranked_schedule_dates as (
    select
        *,
        row_number() over (partition by schedule_window_type order by service_date desc) as date_rank
    from eligible_dates
    where schedule_window_type is not null
),

membership as (
    select
        'day' as window_type,
        cast(anchor.source_end_date as string) as window_key,
        date('{{ processing_date }}') as source_end_date,
        anchor.source_end_date as service_date,
        observed_dates.schedule_day_type,
        1 as date_rank
    from (select date('{{ processing_date }}') as source_end_date) as anchor
    left join observed_dates
        on observed_dates.service_date = anchor.source_end_date

    union all

    select
        'month',
        format_date('%Y-%m', date('{{ processing_date }}')),
        date('{{ processing_date }}'),
        service_date,
        schedule_day_type,
        row_number() over (order by service_date desc)
    from observed_dates
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)

    union all

    select
        schedule_window_type,
        cast(date('{{ processing_date }}') as string),
        date('{{ processing_date }}'),
        service_date,
        schedule_day_type,
        date_rank
    from ranked_schedule_dates
    where date_rank <= {{ matching_date_count }}
)

select *
from membership
