
    
    

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


