{{ config(tags=['audit']) }}

select
    schedule_version_id,
    valid_from_date,
    valid_to_date,
    first_processing_date,
    last_processing_date
from {{ ref('int_schedule_version') }}
where valid_to_date < valid_from_date
   or first_processing_date != valid_from_date
   or last_processing_date < first_processing_date
