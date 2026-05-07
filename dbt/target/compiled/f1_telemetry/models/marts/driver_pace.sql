select
    race_year,
    race_round,
    session,
    driver,
    cast(avg(lap_time_ms) as double) as avg_lap_time_ms,
    cast(min(lap_time_ms) as bigint) as fastest_lap_ms,
    cast(stddev_samp(lap_time_ms) as double) as lap_std_dev_ms,
    cast(count(*) as bigint) as laps_completed
from "warehouse"."main_silver"."stg_laps"
group by race_year, race_round, session, driver