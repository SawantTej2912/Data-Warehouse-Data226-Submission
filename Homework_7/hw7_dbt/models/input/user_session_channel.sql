with src as (
  select userId, sessionId, channel
  from USER_DB_PARROT.RAW.USER_SESSION_CHANNEL
)
select * from src