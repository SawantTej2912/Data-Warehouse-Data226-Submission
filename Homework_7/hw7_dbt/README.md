# Homework 7 — dbt + Snowflake

Project builds input views from RAW tables, creates `ANALYTICS.SESSION_SUMMARY`, snapshots it, and tests `SESSIONID` (not_null, unique).

## How to run
```bash
dbt debug
dbt run -s "path:models/input/*"
dbt run -s output.session_summary
dbt snapshot
dbt test -s session_summary
