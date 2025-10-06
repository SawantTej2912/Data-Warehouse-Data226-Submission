from airflow.decorators import dag, task
from datetime import datetime, timedelta
import requests

# Default arguments for the DAG
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="hw4_step1_tasks_and_dependencies",
    description="Step 1 of HW4: Use @task and set dependencies",
    schedule_interval="@daily",          # runs once per day
    start_date=datetime(2025, 1, 1),     # pick a safe start date in the past
    catchup=False,
    default_args=default_args,
    tags=["hw4", "step1"],
)
def hw4_step1():

    
    @task
    def extract(symbol: str = "AAPL"):
        api_key = Variable.get("AlphaVantage_API_KEY")   # <-- new
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        r = requests.get(url)
        data = r.json()
        return data

    @task
    def transform(records):
        print(f"[transform] Got {len(records)} rows")
        return records

    @task
    def load(records):
        print(f"[load] Would insert {len(records)} rows into Snowflake (stub)")

    # Define task dependencies: extract -> transform -> load
    extracted = extract("AAPL")
    transformed = transform(extracted)
    load(transformed)

# Expose the DAG to Airflow
hw4_step1 = hw4_step1()
