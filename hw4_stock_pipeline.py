from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime, timedelta
import requests

# ---------------------------
# Default args
# ---------------------------
default_args = {
    "owner": "student",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------------------------
# DAG Definition
# ---------------------------
@dag(
    dag_id="hw4_stock_pipeline",
    description="Homework 4 ported to Airflow: Alpha Vantage -> Snowflake",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["HW4", "AlphaVantage", "Snowflake"],
)
def hw4_pipeline():

    # ---------------------------
    # Task: Extract
    # ---------------------------
    @task
    def extract(symbol: str = "AAPL"):
        api_key = Variable.get("AlphaVantage_API_KEY")
        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={api_key}"
        r = requests.get(url, timeout=30)
        data = r.json()
        return data

    # ---------------------------
    # Task: Transform
    # ---------------------------
    @task
    def transform(data):
        cutoff_date = datetime.now() - timedelta(days=90)
        results = []
        for d, stock_info in data["Time Series (Daily)"].items():
            date_obj = datetime.strptime(d, "%Y-%m-%d")
            if date_obj >= cutoff_date:
                results.append({
                    "date": d,
                    "open": stock_info["1. open"],
                    "high": stock_info["2. high"],
                    "low": stock_info["3. low"],
                    "close": stock_info["4. close"],
                    "volume": stock_info["5. volume"]
                })
        return results

    # ---------------------------
    # Task: Load (Transactional)
    # ---------------------------
    @task
    def load(records, target_table="RAW.STOCK_PRICE", symbol="AAPL"):
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute("BEGIN;")
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {target_table} (
                    symbol VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    open DECIMAL(12,4) NOT NULL,
                    close DECIMAL(12,4) NOT NULL,
                    high DECIMAL(12,4) NOT NULL,
                    low DECIMAL(12,4) NOT NULL,
                    volume BIGINT NOT NULL,
                    PRIMARY KEY (symbol, date)
                )
            """)
            cur.execute(f"DELETE FROM {target_table}")
            for r in records:
                insert_sql = f"""
                INSERT INTO {target_table} (symbol, date, open, close, high, low, volume)
                VALUES ('{symbol}', '{r["date"]}', {r["open"]}, {r["close"]}, {r["high"]}, {r["low"]}, {r["volume"]})
                """
                cur.execute(insert_sql)
            cur.execute("COMMIT;")
        except Exception as e:
            cur.execute("ROLLBACK;")
            raise e
        finally:
            cur.close()
            conn.close()
        return len(records)

    # ---------------------------
    # DAG Flow
    # ---------------------------
    data = extract("AAPL")
    transformed = transform(data)
    load(transformed)

hw4_pipeline = hw4_pipeline()
