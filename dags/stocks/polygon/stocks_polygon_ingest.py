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
    def create_master_ticker_file(**kwargs) -> str:
        dag_run = kwargs.get("dag_run")
        conf_tickers = dag_run.conf.get('tickers')
        if conf_tickers and isinstance(conf_tickers, list):
            all_tickers = conf_tickers
        else:
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
        s3_hook.load_string(string_data=tickers_string, key=MASTER_TICKERS_FILE_KEY, bucket_name=BUCKET_NAME, replace=True)
        return MASTER_TICKERS_FILE_KEY

    @task
    def split_file_into_batches(master_file_key: str) -> list[str]:
        BATCH_SIZE = 1000
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=master_file_key, bucket_name=BUCKET_NAME)
        all_tickers = tickers_string.splitlines()
        batch_file_keys = []
        for i in range(0, len(all_tickers), BATCH_SIZE):
            batch = all_tickers[i:i + BATCH_SIZE]
            batch_string = "\n".join(batch)
            batch_file_key = f"batches/tickers_batch_{i // BATCH_SIZE + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)
        return batch_file_keys

    @task
    def process_ticker_batch(batch_s3_key: str, execution_date: str) -> list[str]:
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        tickers_in_batch = tickers_string.splitlines()
        processed_s3_keys = []
        for ticker in tickers_in_batch:
            is_dev = Variable.get("development_mode", default_var="true").lower() == "true"
            target_date = pendulum.parse(execution_date).subtract(years=2 if is_dev else 0).subtract(days=1 if not is_dev else 0).to_date_string()
            conn = BaseHook.get_connection('polygon_api')
            api_key = conn.password
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            if data.get('resultsCount', 0) > 0:
                json_string = json.dumps(data)
                s3_key = f"{ticker}_{target_date}.json"
                s3_hook.load_string(string_data=json_string, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
                processed_s3_keys.append(s3_key)
        return processed_s3_keys

    # NEW: A task to trigger one load DAG per batch
    trigger_load_dag_for_batch = TriggerDagRunOperator.partial(
        task_id="trigger_load_dag_for_batch",
        trigger_dag_id="stocks_polygon_load",
        wait_for_completion=False, # Set to True if you want to wait
    ).expand(
        conf=process_ticker_batch.partial(execution_date="{{ ds }}").expand(
            batch_s3_key=split_file_into_batches(create_master_ticker_file())
        ).map(lambda keys: {"s3_keys": keys})
    )

stocks_polygon_ingest_dag()
