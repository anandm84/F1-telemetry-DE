
  
    
    

    create  table
      "warehouse"."main_gold"."dim_circuits__dbt_tmp"
  
    as (
      
with active_years as (
    select distinct race_year from "warehouse"."main_silver"."stg_race_results"
),

active_circuits as (
    select c.*
    from "warehouse"."main_seeds"."circuits" c
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
    );
  
  