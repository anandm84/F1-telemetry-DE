select
    record_id,
    race_year,
    race_round,
    session,
    cast(race_year as varchar) || '_' || cast(race_round as varchar) as race_key,
    driver_id,
    lap_number,
    pit_in_ms,
    pit_out_ms,
    tire_compound_before
from {{ ref('stg_pit_stops') }}
