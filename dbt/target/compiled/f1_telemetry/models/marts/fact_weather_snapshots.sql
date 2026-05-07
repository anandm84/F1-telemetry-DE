select
    record_id,
    race_year,
    race_round,
    session,
    cast(race_year as varchar) || '_' || cast(race_round as varchar) as race_key,
    snapshot_index,
    time_offset_ms,
    air_temp_c,
    track_temp_c,
    humidity_pct,
    pressure_mbar,
    wind_speed_ms,
    is_raining
from "warehouse"."main_silver"."stg_weather"