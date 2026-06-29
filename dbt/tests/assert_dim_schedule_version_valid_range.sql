select schedule_version_id, valid_from_date, valid_to_date
from {{ ref('dim_schedule_version') }}
where valid_to_date is not null
  and valid_to_date < valid_from_date
