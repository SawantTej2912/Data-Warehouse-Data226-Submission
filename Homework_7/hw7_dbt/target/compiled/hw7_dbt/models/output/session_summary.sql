with
user_session_channel as (
  select * from USER_DB_PARROT.ANALYTICS.user_session_channel
),
session_timestamp as (
  select * from USER_DB_PARROT.ANALYTICS.session_timestamp
),
dedup_events as (
  select
    u.userId,
    u.sessionId,
    u.channel,
    s.ts
  from user_session_channel u
  join session_timestamp s
    on u.sessionId = s.sessionId
  qualify row_number() over (partition by u.sessionId, s.ts order by s.ts) = 1
)
select
  userId,
  sessionId,
  channel,
  min(ts) as session_start,
  max(ts) as session_end,
  count(*) as event_count
from dedup_events
group by userId, sessionId, channel