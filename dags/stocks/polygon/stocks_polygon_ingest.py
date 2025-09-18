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
    schedule=None,
    catchup=False,
    tags=["ingestion", "polygon"],
)
def stocks_polygon_ingest_dag():
    """
    [Placeholder] This DAG is triggered by the controller and will
    eventually fetch data for a single ticker from Polygon.io.
    """

    @task
    def process_ticker(**kwargs):
        dag_run = kwargs.get("dag_run")
        ticker = dag_run.conf.get('ticker', 'N/A')
        print(f"Received ticker to process: {ticker}")

    process_ticker()
    """
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
    """
stocks_polygon_ingest_dag()
