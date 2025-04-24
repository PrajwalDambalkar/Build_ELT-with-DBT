select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select moving_avg_5d
from USER_DB_BOA.analytics.moving_avg
where moving_avg_5d is null



      
    ) dbt_internal_test