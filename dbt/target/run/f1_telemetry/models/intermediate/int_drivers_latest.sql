
  
    
    

    create  table
      "warehouse"."main_intermediate"."int_drivers_latest__dbt_tmp"
  
    as (
      with ranked as (
    select
        driver_id,
        full_name,
        team,
        nationality,
        race_year,
        race_round,
        row_number() over (
            partition by driver_id
            order by race_year desc, race_round desc
        ) as rn
    from "warehouse"."main_silver"."stg_race_results"
    where driver_id is not null
      and driver_id <> ''
)

select
    driver_id,
    full_name,
    team,
    nationality
from ranked
where rn = 1
    );
  
  