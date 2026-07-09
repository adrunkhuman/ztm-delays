with grouped_pings as (
    select
        vehicle_number,
        gps_time,
        count(*) as row_count,
        countif(vehicle_type not in (1, 2)) as vehicle_type_violations
    from {{ ref('stg_gps__pings') }}
    where gps_date = date('{{ var("processing_date", "1970-01-01") }}')
    group by vehicle_number, gps_time
),

contract_counts as (
    select
        countif(row_count > 1) as duplicate_vehicle_time_violations,
        sum(vehicle_type_violations) as vehicle_type_violations
    from grouped_pings
)

select issue_type, violation_count
from contract_counts
cross join unnest([
    struct('unique_vehicle_time' as issue_type, duplicate_vehicle_time_violations as violation_count),
    struct('vehicle_type_accepted_values' as issue_type, vehicle_type_violations as violation_count)
])
where violation_count > 0
