
  
    
    

    create  table
      "warehouse"."main_gold"."tire_performance__dbt_tmp"
  
    as (
      select
    race_year,
    race_round,
    session,
    tire_compound,
    cast(avg(lap_time_ms) as double) as avg_lap_time_ms,
    cast(count(*) as bigint) as laps_run
from "warehouse"."main_silver"."stg_laps"
group by race_year, race_round, session, tire_compound
    );
  
  