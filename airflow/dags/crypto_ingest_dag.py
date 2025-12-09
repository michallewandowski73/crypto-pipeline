from datetime import datetime, timedelta
import requests
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator

POSTGRES_CONN = {
    "host": "postgres",
    "dbname": "crypto",
    "user": "airflow",
    "password": "airflow",
    "port": 5432,
}

COINS = ["bitcoin", "ethereum"]
VS_CURRENCY = "usd"


def fetch_and_load():
    url = "https://api.coingecko.com/api/v3/coins/markets"
    params = {
        "vs_currency": VS_CURRENCY,
        "ids": ",".join(COINS),
    }
    resp = requests.get(url, params=params, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    rows = []
    for item in data:
        rows.append(
            (
                item["symbol"],
                VS_CURRENCY,
                item["current_price"],
                item.get("market_cap"),
                item.get("total_volume"),
            )
        )

    conn = psycopg2.connect(**POSTGRES_CONN)
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.executemany(
            """
            INSERT INTO crypto_prices_raw
                (symbol, currency, price, market_cap, volume_24h)
            VALUES (%s, %s, %s, %s, %s)
            """,
            rows,
        )
    conn.close()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="crypto_price_ingest",
    default_args=default_args,
    schedule_interval="*/5 * * * *",  # every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["demo", "crypto"],
) as dag:
    ingest_task = PythonOperator(
        task_id="fetch_and_load_crypto_prices",
        python_callable=fetch_and_load,
    )
