
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select borough
from "new_york"."public_public"."fct_incidents"
where borough is null



  
  
      
    ) dbt_internal_test