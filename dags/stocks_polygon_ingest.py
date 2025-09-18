import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "alphavantage"],
)
def stocks_polygon_ingest_dag():
    
    @task(retries=2)
    def fetch_and_save_daily_data() -> str:
        TICKER = os.getenv("TICKER", "GOOGL")
        api_key = os.getenv("ALPHA_ADVANTAGE_API_KEY")
        output_path = f"/tmp/{TICKER}_daily.json"

        if not api_key:
            raise ValueError("ALPHA_ADVANTAGE_API_KEY environment variable not set.")

        url = (
            "https://www.alphavantage.co/query?"
            "function=TIME_SERIES_DAILY&"
            f"symbol={TICKER}&"
            "outputsize=full&"
            f"apikey={api_key}"
        )
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        if "Time Series (Daily)" not in data:
            print("API did not return Time Series Data. Full response:")
            print(data)
            raise ValueError("Alpha Vantage API call did not return the expected time series data.")

        with open(output_path, "w") as f:
            json.dump(data, f)
        
        return output_path

    @task
    def upload_to_minio(local_file_path: str) -> str:
        S3_CONN_ID = os.getenv("S3_CONN_ID")
        BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
        s3_key = os.path.basename(local_file_path)

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        print(f"Successfully uploaded {s3_key} to Minio bucket {BUCKET_NAME}.")
        return s3_key

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="stocks_polygon_load",
        wait_for_completion=False,
        conf={"s3_key": "{{ task_instance.xcom_pull(task_ids='upload_to_minio') }}"}
    )

    local_path = fetch_and_save_daily_data()
    s3_key_from_upload = upload_to_minio(local_file_path=local_path)
    s3_key_from_upload >> trigger_load_dag

stocks_polygon_ingest_dag()
