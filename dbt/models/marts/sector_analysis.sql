select
    race_year,
    race_round,
    session,
    driver,
    cast(avg(sector1_ms) as double) as avg_sector1_ms,
    cast(avg(sector2_ms) as double) as avg_sector2_ms,
    cast(avg(sector3_ms) as double) as avg_sector3_ms
from {{ ref('stg_laps') }}
group by race_year, race_round, session, driver
