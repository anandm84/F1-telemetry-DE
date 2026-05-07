
  
    
    

    create  table
      "warehouse"."main_gold"."dim_drivers__dbt_tmp"
  
    as (
      select
    driver_id,
    full_name,
    team,
    nationality,
    current_timestamp as _updated_at
from "warehouse"."main_intermediate"."int_drivers_latest"
    );
  
  