from __future__ import annotations
import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable

@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "polygon"],
)
def stocks_polygon_ingest_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    MASTER_TICKERS_FILE_KEY = "tickers_list.txt"

    @task
    def create_master_ticker_file() -> str:
        """
        Fetches all tickers from Polygon.io, saves them to a single master file in S3,
        and returns the S3 key of that file.
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
        
        tickers_string = "\n".join(all_tickers)
        
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_string(
            string_data=tickers_string,
            key=MASTER_TICKERS_FILE_KEY,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        return MASTER_TICKERS_FILE_KEY

    @task
    def split_file_into_batches(master_file_key: str) -> list[str]:
        """
        Reads the master tickers file from S3, splits it into smaller batch files,
        writes those batches back to S3, and returns a list of the new batch file keys.
        """
        BATCH_SIZE = 1000  # Process 1000 tickers per parallel task
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)

        tickers_string = s3_hook.read_key(key=master_file_key, bucket_name=BUCKET_NAME)
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

    @task
    def process_ticker_batch(batch_s3_key: str, execution_date: str) -> list[str]:
        """
        Reads one batch file from S3, then processes each ticker in that batch,
        uploading the results to S3. Returns a list of S3 keys for the processed data.
        """
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()
        
        processed_s3_keys = []

        for ticker in tickers_in_batch:
            # Re-using the logic from your original fetch_and_save and upload tasks
            # but as simple function calls within this loop.
            is_dev = Variable.get("development_mode", default_var="true").lower() == "true"
            if is_dev:
                target_date = pendulum.parse(execution_date).subtract(years=2).to_date_string()
            else:
                target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()

            conn = BaseHook.get_connection('polygon_api')
            api_key = conn.password
            url = (
                f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
                f"{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
            )
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            if data.get('resultsCount', 0) > 0:
                json_string = json.dumps(data)
                s3_key = f"{ticker}_{target_date}.json"
                s3_hook.load_string(
                    string_data=json_string,
                    key=s3_key,
                    bucket_name=BUCKET_NAME,
                    replace=True
                )
                processed_s3_keys.append(s3_key)
        
        return processed_s3_keys

    @task
    def trigger_load_dag(processed_keys_from_batches: list):
        """
        Flattens the list of lists and triggers the loading DAG with all S3 keys.
        """
        # The input will be a list of lists, e.g., [[key1, key2], [key3, key4]]
        # We need to flatten it into a single list.
        all_valid_s3_keys = [key for batch in processed_keys_from_batches for key in batch if key]
        
        if not all_valid_s3_keys:
            print("No new data to load. Skipping trigger of stocks_polygon_load.")
            return

        TriggerDagRunOperator(
            task_id="trigger_load_dag",
            trigger_dag_id="stocks_polygon_load",
            wait_for_completion=False,
            conf={"s3_keys": all_valid_s3_keys}
        ).execute(context={})

    # The Final, Scalable Task Flow
    master_key = create_master_ticker_file()
    batch_keys = split_file_into_batches(master_key)
    processed_s3_keys = process_ticker_batch.partial(execution_date="{{ ds }}").expand(batch_s3_key=batch_keys)
    trigger_load_dag(processed_s3_keys)

stocks_polygon_ingest_dag()
