
  create or replace   view USER_DB_PARROT.ANALYTICS.session_timestamp
  
  
  
  
  as (
    with src as (
  select sessionId, ts
  from USER_DB_PARROT.RAW.SESSION_TIMESTAMP
)
select * from src
  );

