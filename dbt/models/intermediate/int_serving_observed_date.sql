{% set processing_date = var("processing_date", "1970-01-01") %}

{{
    config(
        materialized='incremental',
        incremental_strategy='insert_overwrite',
        partition_by={"field": "service_date", "data_type": "date"},
        partitions=["date('" ~ processing_date ~ "')"],
        require_partition_filter=true,
        post_hook="alter table {{ this }} set options (require_partition_filter = true)",
    )
}}

select
    service_date,
    any_value(schedule_day_type) as schedule_day_type
from {{ ref('int_serving_stop_arrival') }}
where service_date <= date('{{ processing_date }}')
{% if is_incremental() %}
  and service_date = date('{{ processing_date }}')
{% endif %}
group by service_date
