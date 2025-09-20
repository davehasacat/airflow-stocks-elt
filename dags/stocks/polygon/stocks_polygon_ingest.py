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

# Define the DAG
@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "polygon"],
)
def stocks_polygon_ingest_dag():
    # Define S3 and Bucket connections
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    # Task to get all tickers, split them into batches, and save each batch to S3
    @task
    def get_and_batch_tickers_to_s3(**kwargs) -> list[str]:
        # Get the Polygon.io API key from the Airflow connection
        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("API key not found in Airflow connection 'polygon_api'.")
        
        # Paginate through the API to get all tickers
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

        # Split tickers into batches and save each to S3
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        batch_size = 500  # Define a manageable batch size
        batch_file_keys = []
        for i in range(0, len(all_tickers), batch_size):
            batch = all_tickers[i:i + batch_size]
            batch_string = "\n".join(batch)
            batch_file_key = f"batches/tickers_batch_{i // batch_size + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)
            
        # Return the list of S3 keys for the batches. This list is small enough for XComs.
        return batch_file_keys

    # Task to process a batch of tickers from a file in S3
    @task(retries=3, retry_delay=pendulum.duration(minutes=5))
    def process_ticker_batch(batch_s3_key: str, execution_date: str) -> list[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        
        # Read the tickers for this batch from S3
        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()
        
        processed_s3_keys = []
        # Loop through each ticker in the batch and process it
        for ticker in tickers_in_batch:
            is_dev = Variable.get("development_mode", default_var="true").lower() == "true"
            target_date = pendulum.parse(execution_date).subtract(years=2 if is_dev else 0).subtract(days=1 if not is_dev else 0).to_date_string()
            
            conn = BaseHook.get_connection('polygon_api')
            api_key = conn.password
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
            
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            
            # If data is returned, save it to S3 as a JSON file
            if data.get('resultsCount', 0) > 0:
                json_string = json.dumps(data)
                s3_key = f"raw_data/{ticker}_{target_date}.json"
                s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                processed_s3_keys.append(s3_key)

        return processed_s3_keys

    # Task to trigger the downstream 'stocks_polygon_load' DAG
    @task
    def trigger_load_dag(s3_keys_nested: list[list[str]]):
        # Flatten the list of lists into a single list of S3 keys
        all_s3_keys = [key for sublist in s3_keys_nested for key in sublist]

        # If there are valid S3 keys, trigger the next DAG
        if all_s3_keys:
            TriggerDagRunOperator(
                task_id="trigger_load_dag_run", # Renamed task_id to be unique
                trigger_dag_id="stocks_polygon_load",
                conf={"s3_keys": all_s3_keys},
            ).execute(context={})

    # Define the DAG's task dependencies
    batch_keys = get_and_batch_tickers_to_s3()
    processed_keys = process_ticker_batch.partial(execution_date="{{ ds }}").expand(batch_s3_key=batch_keys)
    trigger_load_dag(s3_keys_nested=processed_keys)

# Instantiate the DAG
stocks_polygon_ingest_dag()
