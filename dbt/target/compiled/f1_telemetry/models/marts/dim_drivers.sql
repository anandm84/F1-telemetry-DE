select
    driver_id,
    full_name,
    team,
    nationality,
    current_timestamp as _updated_at
from "warehouse"."main_intermediate"."int_drivers_latest"