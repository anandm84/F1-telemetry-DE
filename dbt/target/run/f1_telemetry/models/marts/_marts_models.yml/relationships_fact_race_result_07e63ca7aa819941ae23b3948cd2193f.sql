
    
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with child as (
    select driver_id as from_field
    from "warehouse"."main_gold"."fact_race_results"
    where driver_id is not null
),

parent as (
    select driver_id as to_field
    from "warehouse"."main_gold"."dim_drivers"
)

select
    from_field

from child
left join parent
    on child.from_field = parent.to_field

where parent.to_field is null



  
  
      
    ) dbt_internal_test