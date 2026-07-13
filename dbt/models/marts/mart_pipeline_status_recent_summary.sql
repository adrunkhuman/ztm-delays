{{ config(materialized='table', cluster_by=["mode"]) }}

with recent as (
    select *
    from {{ ref('mart_pipeline_status') }}
    where service_date >= date_sub(date('{{ var("processing_date", "1970-01-01") }}'), interval 30 day)
    qualify row_number() over (partition by mode order by service_date desc) <= 8
),

grouped as (
    select
        mode,
        count(*) as day_count,
        min(service_date) as first_date,
        max(service_date) as last_date,
        safe_divide(sum(completeness_ratio * stop_arrivals_count), nullif(sum(if(completeness_ratio is not null, stop_arrivals_count, 0)), 0)) as completeness_ratio,
        safe_divide(sum(observed_service_minutes), nullif(sum(expected_service_minutes), 0)) as service_coverage_ratio,
        sum(trips_complete) as trips_complete,
        sum(trips_broken) as trips_broken,
        sum(expected_service_minutes) as expected_service_minutes,
        sum(observed_service_minutes) as observed_service_minutes
    from recent
    group by mode
)

select
    *,
    least(coalesce(completeness_ratio, 1.0), coalesce(service_coverage_ratio, 1.0)) as health_ratio,
    case
        when completeness_ratio is null and service_coverage_ratio is null then 'no data'
        when least(coalesce(completeness_ratio, 1.0), coalesce(service_coverage_ratio, 1.0)) >= 0.9 then 'good'
        when least(coalesce(completeness_ratio, 1.0), coalesce(service_coverage_ratio, 1.0)) >= 0.7 then 'usable'
        when least(coalesce(completeness_ratio, 1.0), coalesce(service_coverage_ratio, 1.0)) > 0 then 'patchy'
        else 'missing'
    end as health_label
from grouped
