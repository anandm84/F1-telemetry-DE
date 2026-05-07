
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select race_round
from "warehouse"."main_silver"."stg_race_results"
where race_round is null



  
  
      
    ) dbt_internal_test