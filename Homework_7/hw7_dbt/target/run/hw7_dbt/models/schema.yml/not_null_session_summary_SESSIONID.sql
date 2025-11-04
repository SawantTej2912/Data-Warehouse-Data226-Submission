
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select SESSIONID
from USER_DB_PARROT.ANALYTICS.session_summary
where SESSIONID is null



  
  
      
    ) dbt_internal_test