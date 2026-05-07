select
    record_id,
    race_year,
    race_round,
    session,
    cast(race_year as varchar) || '_' || cast(race_round as varchar) as race_key,
    driver_id,
    full_name,
    team,
    position,
    grid_position,
    points,
    status
from "warehouse"."main_silver"."stg_race_results"