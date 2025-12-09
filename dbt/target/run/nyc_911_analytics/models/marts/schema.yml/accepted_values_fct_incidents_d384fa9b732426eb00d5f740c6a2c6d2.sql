
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

with all_values as (

    select
        borough as value_field,
        count(*) as n_records

    from "new_york"."public_public"."fct_incidents"
    group by borough

)

select *
from all_values
where value_field not in (
    'MANHATTAN','BRONX','BROOKLYN','QUEENS','STATEN ISLAND'
)



  
  
      
    ) dbt_internal_test