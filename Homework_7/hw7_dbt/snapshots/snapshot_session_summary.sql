{% snapshot snapshot_session_summary %}
{{
  config(
    target_database='USER_DB_PARROT',
    target_schema='ANALYTICS',
    unique_key='SESSIONID',
    strategy='check',
    check_cols=['SESSION_START','SESSION_END','EVENT_COUNT']
  )
}}
select
  SESSIONID,
  USERID,
  CHANNEL,
  SESSION_START,
  SESSION_END,
  EVENT_COUNT
from {{ ref('session_summary') }}
{% endsnapshot %}
