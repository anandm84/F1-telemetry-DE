select
    driver_id,
    full_name,
    team,
    nationality,
    current_timestamp as _updated_at
from {{ ref('int_drivers_latest') }}
