{% set processing_date = var("processing_date", "1970-01-01") %}

{{ config(materialized='table', tags=['audit']) }}

with latest_trip_universe as (
    select *
    from {{ ref('int_serving_trip_universe') }}
    where processing_date = date('{{ processing_date }}')
),

window_trip_universe as (
    select *
    from {{ ref('int_serving_trip_universe') }}
    where processing_date between date_sub(date('{{ processing_date }}'), interval 60 day)
        and date('{{ processing_date }}')
),

summary_rows as (
    select
        'trip_universe' as evidence_section,
        cast(null as string) as entity_type,
        cast(null as string) as window_type,
        count(*) as total_count,
        countif(is_public_service_segment) as public_service_count,
        countif(is_short_turn_part_trip) as short_turn_part_trip_count,
        countif(is_zone1_only) as zone1_only_count,
        countif(is_zone1_public_ranking_trip) as ranking_trip_count,
        cast(null as int64) as below_min_arrival_entities
    from latest_trip_universe
),

arrival_base as (
    select
        arrivals.*,
        window_trip_universe.is_zone1_public_ranking_trip
    from {{ ref('int_serving_stop_arrival') }} as arrivals
    inner join window_trip_universe
        on arrivals.gtfs_snapshot_id = window_trip_universe.gtfs_snapshot_id
        and arrivals.gps_date = window_trip_universe.processing_date
        and arrivals.service_date = window_trip_universe.service_date
        and arrivals.trip_id = window_trip_universe.trip_id
    where arrivals.service_date between date_sub(date('{{ processing_date }}'), interval 60 day)
        and date('{{ processing_date }}')
      and arrivals.trip_quality = 'complete'
      and window_trip_universe.is_zone1_public_ranking_trip
),

windowed_entities as (
    select 'line' as entity_type, line as entity_id, 'day' as window_type, 1 as source_day_count, count(*) as arrival_count
    from arrival_base
    where service_date = date('{{ processing_date }}')
    group by entity_type, entity_id, window_type, source_day_count

    union all

    select 'stop_group', stop_group_id, 'day', 1, count(*)
    from arrival_base
    where service_date = date('{{ processing_date }}')
    group by stop_group_id

    union all

    select 'stop_post', stop_id, 'day', 1, count(*)
    from arrival_base
    where service_date = date('{{ processing_date }}')
    group by stop_id

    union all

    select 'line', line, 'month', count(distinct service_date), count(*)
    from arrival_base
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)
    group by line

    union all

    select 'stop_group', stop_group_id, 'month', count(distinct service_date), count(*)
    from arrival_base
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)
    group by stop_group_id

    union all

    select 'stop_post', stop_id, 'month', count(distinct service_date), count(*)
    from arrival_base
    where date_trunc(service_date, month) = date_trunc(date('{{ processing_date }}'), month)
    group by stop_id

    union all

    select 'line', line, 'weekdays', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
    group by line

    union all

    select 'stop_group', stop_group_id, 'weekdays', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
    group by stop_group_id

    union all

    select 'stop_post', stop_id, 'weekdays', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('monday', 'tuesday', 'wednesday', 'thursday', 'friday', 'weekday')
    group by stop_id

    union all

    select 'line', line, 'weekend', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('saturday', 'sunday_holiday')
    group by line

    union all

    select 'stop_group', stop_group_id, 'weekend', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('saturday', 'sunday_holiday')
    group by stop_group_id

    union all

    select 'stop_post', stop_id, 'weekend', count(distinct service_date), count(*)
    from arrival_base
    where schedule_day_type in ('saturday', 'sunday_holiday')
    group by stop_id
),

eligibility as (
    select
        entity_type,
        window_type,
        count(*) as total_count,
        countif(arrival_count < case entity_type when 'line' then 20 else 10 end * source_day_count)
            as below_min_arrival_entities
    from windowed_entities
    group by entity_type, window_type
)

select * from summary_rows
union all
select
    'rank_eligibility' as evidence_section,
    entity_type,
    window_type,
    total_count,
    cast(null as int64) as public_service_count,
    cast(null as int64) as short_turn_part_trip_count,
    cast(null as int64) as zone1_only_count,
    cast(null as int64) as ranking_trip_count,
    below_min_arrival_entities
from eligibility
