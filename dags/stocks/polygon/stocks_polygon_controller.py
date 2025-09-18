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
    This DAG fetches a dynamic list of tickers, saves it to Minio, splits it
    into batches, and triggers a batch processing DAG for each batch file.
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

        tickers_string = "\n".join(all_tickers)
        
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_string(
            string_data=tickers_string,
            key=TICKERS_FILE_KEY,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        return TICKERS_FILE_KEY

    @task
    def split_tickers_into_batches(file_key: str) -> list[str]:
        """
        Reads the full list of tickers from Minio, splits it into smaller
        batches, and writes each batch back to Minio as a separate file.
        Returns a list of the new batch file keys.
        """
        BATCH_SIZE = 1000
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        tickers_string = s3_hook.read_key(key=file_key, bucket_name=BUCKET_NAME)
        all_tickers = tickers_string.splitlines()

        batch_file_keys = []
        for i in range(0, len(all_tickers), BATCH_SIZE):
            batch = all_tickers[i:i + BATCH_SIZE]
            batch_string = "\n".join(batch)
            batch_file_key = f"batches/tickers_batch_{i // BATCH_SIZE + 1}.txt"
            
            s3_hook.load_string(
                string_data=batch_string,
                key=batch_file_key,
                bucket_name=BUCKET_NAME,
                replace=True
            )
            batch_file_keys.append(batch_file_key)

        return batch_file_keys

    trigger_batch_processor_dags = TriggerDagRunOperator.partial(
        task_id="trigger_batch_processor_dags",
        trigger_dag_id="stocks_polygon_batch_processor",
    ).expand(
        conf=split_tickers_into_batches(
            file_key=fetch_and_save_tickers_to_minio()
        ).map(lambda file_key: {"batch_file_key": file_key}) # Pass the batch filename
    )

stocks_polygon_controller_dag()
