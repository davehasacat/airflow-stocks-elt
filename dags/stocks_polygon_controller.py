from __future__ import annotations
import pendulum
import requests
import time
import os
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id="stocks_polygon_controller",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["controller", "ingestion", "polygon"],
)
def stocks_polygon_controller_dag():
    """
    This DAG fetches a dynamic list of tickers, saves it to Minio to handle
    large volume, then reads the list to trigger the ingest DAG for each ticker.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    TICKERS_FILE_KEY = "tickers_list.txt"

    @task
    def fetch_and_save_tickers_to_minio() -> str:
        """
        Fetches all tickers from Polygon.io and saves them to a file in Minio.
        Returns the S3 key of the file.
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
            
            all_tickers.extend(item["ticker"] for item in data.get('results', []))
            
            next_url = data.get('next_url')
            if next_url:
                next_url += f"&apiKey={api_key}"
            
            time.sleep(12)

        # Convert the list to a single string with newlines
        tickers_string = "\n".join(all_tickers)
        
        # Save the string to a file in Minio
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_string(
            string_data=tickers_string,
            key=TICKERS_FILE_KEY,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"Successfully saved {len(all_tickers)} tickers to {TICKERS_FILE_KEY} in bucket {BUCKET_NAME}.")
        return TICKERS_FILE_KEY

    @task
    def read_tickers_from_minio(file_key: str) -> list[str]:
        """
        Reads the list of tickers from the file in Minio.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=file_key, bucket_name=BUCKET_NAME)
        # Split the string back into a list of tickers
        all_tickers = tickers_string.splitlines()
        
        # Only process the first 10 tickers for testing purposes.
        # Remember to remove this slice before deploying to production.
        # return all_tickers[:10]

    # Define the trigger operator
    trigger_ingest_dags = TriggerDagRunOperator.partial(
        task_id="trigger_ingest_dags",
        trigger_dag_id="stocks_polygon_ingest",
    ).expand(
        conf=read_tickers_from_minio(
            file_key=fetch_and_save_tickers_to_minio()
        ).map(lambda ticker: {"ticker": ticker})
    )

stocks_polygon_controller_dag()
