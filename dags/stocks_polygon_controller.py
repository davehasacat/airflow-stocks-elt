from __future__ import annotations
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook

@dag(
    dag_id="stocks_polygon_controller",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["controller", "ingestion", "polygon"],
)
def stocks_polygon_controller_dag():
    """
    This DAG fetches a dynamic list of all stock tickers from the Polygon.io API
    and then triggers the 'stocks_ingest_polygon' DAG for each one.
    """

    @task(retries=2)
    def get_all_ticker_symbols() -> list[str]: # MODIFICATION 1: Change the return type hint
        """
        Fetches the complete list of stock tickers from the Polygon.io API,
        filters for active, common stocks, and returns a list of ticker symbols.
        """
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("API key not found in Airflow connection 'polygon_api'.")

        all_tickers = []
        next_url = f"https://api.polygon.io/v3/reference/tickers?active=true&market=stocks&type=CS&limit=1000&apiKey={api_key}"

        while next_url:
            response = requests.get(next_url)
            response.raise_for_status()
            data = response.json()
            
            all_tickers.extend(data.get('results', []))
            
            next_url = data.get('next_url')
            if next_url:
                next_url += f"&apiKey={api_key}"
        
        # MODIFICATION 2: Return a simple list of strings, not dictionaries
        return [item["ticker"] for item in all_tickers]

    trigger_ingest_dags = TriggerDagRunOperator.partial(
        task_id="trigger_ingest_dags",
        trigger_dag_id="stocks_polygon_ingest",
    ).expand(
        # MODIFICATION 3: Use .map() to build the conf for each ticker
        conf=get_all_ticker_symbols().map(lambda ticker: {"ticker": ticker})
    )

stocks_polygon_controller_dag()
