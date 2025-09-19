from __future__ import annotations
import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook

@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is only triggered by other DAGs
    catchup=False,
    tags=["ingestion", "polygon", "worker"],
)
def stocks_polygon_ingest_dag():
    """
    Worker DAG: Extracts daily data for a single, dynamically provided ticker,
    lands the raw JSON in Minio, and then triggers the loading DAG.
    """
    S3_CONN_ID = os.getenv("S3_CONN_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    @task(retries=2)
    def fetch_and_save_daily_data(**kwargs) -> str:
        """
        Fetches one day of aggregate bar data from Polygon.io for a single ticker.
        """
        dag_run = kwargs.get("dag_run")
        ticker = dag_run.conf.get('ticker')
        if not ticker:
            raise ValueError("Ticker was not provided in the DAG run configuration.")

        # --- THIS IS THE FIX ---
        # For testing on the free plan, we must request older historical data.
        # This logic fetches data from exactly two years before the DAG's logical run date.
        # In production with a paid plan, you would revert to the `subtract(days=1)` logic.
        execution_date = kwargs['ds']
        target_date = pendulum.parse(execution_date).subtract(years=2).to_date_string()
        
        output_path = f"/tmp/{ticker}_{target_date}.json"

        conn = BaseHook.get_connection('polygon_api')
        api_key = conn.password
        if not api_key:
            raise ValueError("API key not found in Airflow connection 'polygon_api'.")

        url = (
            f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/"
            f"{target_date}/{target_date}?adjusted=true&sort=asc&apiKey={api_key}"
        )
        
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        with open(output_path, "w") as f:
            json.dump(data, f)
        
        print(f"Successfully saved data for {ticker} on {target_date} to {output_path}")
        return output_path

    @task
    def upload_to_minio(local_file_path: str) -> str:
        """
        Uploads the given file to Minio and returns the S3 key (filename).
        """
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

    # This trigger kicks off the final loading and transformation step.
    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="stocks_polygon_load",
        wait_for_completion=False,
        # Pass the filename from the upload task to the triggered DAG
        conf={"s3_key": "{{ task_instance.xcom_pull(task_ids='upload_to_minio') }}"}
    )

    # Define the task dependencies
    local_path = fetch_and_save_daily_data()
    s3_key_value = upload_to_minio(local_file_path=local_path)
    s3_key_value >> trigger_load_dag

stocks_polygon_ingest_dag()
