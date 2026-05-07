select
    race_year,
    race_round,
    session,
    tire_compound,
    cast(avg(lap_time_ms) as double) as avg_lap_time_ms,
    cast(count(*) as bigint) as laps_run
from {{ ref('stg_laps') }}
group by race_year, race_round, session, tire_compound
