from __future__ import annotations
import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable # <-- Import the Variable class

@dag(
    dag_id="stocks_polygon_ingest",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon", "worker"],
)
def stocks_polygon_ingest_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    @task(retries=2)
    def fetch_and_save_daily_data(**kwargs) -> str:
        dag_run = kwargs.get("dag_run")
        ticker = dag_run.conf.get('ticker')
        if not ticker:
            raise ValueError("Ticker was not provided in the DAG run configuration.")

        execution_date = kwargs['ds']
        
        # --- BEST PRACTICE: Use an Airflow Variable for configuration ---
        is_dev = Variable.get("development_mode", default_var="true").lower() == "true"
        
        if is_dev:
            # For testing on the free plan, fetch older historical data
            target_date = pendulum.parse(execution_date).subtract(years=2).to_date_string()
        else:
            # In production with a paid plan, fetch yesterday's data
            target_date = pendulum.parse(execution_date).subtract(days=1).to_date_string()

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
        
        return output_path

    @task
    def upload_to_minio(local_file_path: str) -> str:
        s3_key = os.path.basename(local_file_path)
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_file(filename=local_file_path, key=s3_key, bucket_name=BUCKET_NAME, replace=True)
        return s3_key

    trigger_load_dag = TriggerDagRunOperator(
        task_id="trigger_load_dag",
        trigger_dag_id="stocks_polygon_load",
        wait_for_completion=False,
        conf={"s3_key": "{{ task_instance.xcom_pull(task_ids='upload_to_minio') }}"}
    )

    local_path = fetch_and_save_daily_data()
    s3_key_value = upload_to_minio(local_file_path=local_path)
    s3_key_value >> trigger_load_dag

stocks_polygon_ingest_dag()
