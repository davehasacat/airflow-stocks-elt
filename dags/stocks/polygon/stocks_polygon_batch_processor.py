from __future__ import annotations
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

@dag(
    dag_id="stocks_polygon_batch_processor",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is only triggered by other DAGs
    catchup=False,
    tags=["processor", "ingestion", "polygon"],
)
def stocks_polygon_batch_processor_dag():
    """
    This DAG is triggered by the main controller for a single batch of tickers.
    It reads the batch file from Minio and triggers the final ingest DAG
    for each ticker in the batch.
    """
    # --- FIX: Added a default value for the S3 connection ID ---
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")

    @task
    def read_tickers_from_batch_file(**kwargs) -> list[str]:
        """
        Reads a specific batch file from Minio to get a list of tickers.
        The filename is passed via the DAG run configuration.
        """
        dag_run = kwargs.get("dag_run")
        batch_file_key = dag_run.conf.get('batch_file_key')
        if not batch_file_key:
            raise ValueError("Batch file key was not provided in the DAG run configuration.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        tickers_string = s3_hook.read_key(key=batch_file_key, bucket_name=BUCKET_NAME)
        
        tickers = tickers_string.splitlines()
        print(f"Found {len(tickers)} tickers in this batch.")
        return tickers

    trigger_final_ingest_dags = TriggerDagRunOperator.partial(
        task_id="trigger_final_ingest_dags",
        trigger_dag_id="stocks_polygon_ingest",
    ).expand(
        conf=read_tickers_from_batch_file().map(lambda ticker: {"ticker": ticker})
    )

stocks_polygon_batch_processor_dag()
