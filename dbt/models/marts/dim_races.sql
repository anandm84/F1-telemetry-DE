{# Restrict the calendar to seasons that actually have observed race results,
   matching the behavior of the original gold_dimensions_job.py. #}
with active_years as (
    select distinct race_year from {{ ref('stg_race_results') }}
)

select
    r.race_key,
    r.race_year,
    r.race_round,
    r.race_name,
    r.circuit_id,
    r.race_date
from {{ ref('races') }} r
inner join active_years a on a.race_year = r.race_year
qualify row_number() over (partition by r.race_key order by r.race_year desc, r.race_round desc) = 1
