{# Distinct circuits across the seasons that actually have observed race results.
   Latest seed_year wins per circuit_id, mirroring gold_dimensions_job.py. #}
with active_years as (
    select distinct race_year from {{ ref('stg_race_results') }}
),

active_circuits as (
    select c.*
    from {{ ref('circuits') }} c
    inner join active_years a on a.race_year = c.seed_year
)

select
    circuit_id,
    circuit_name,
    country,
    locality,
    current_timestamp as _updated_at
from active_circuits
qualify row_number() over (partition by circuit_id order by seed_year desc) = 1
