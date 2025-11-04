
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    

select
    SESSIONID as unique_field,
    count(*) as n_records

from USER_DB_PARROT.ANALYTICS.session_summary
where SESSIONID is not null
group by SESSIONID
having count(*) > 1



  
  
      
    ) dbt_internal_test