from __future__ import annotations
import pendulum
import os
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.exceptions import AirflowSkipException

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["loading", "polygon", "postgres"],
)
def stocks_polygon_load_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    TABLE_NAME = "source_stocks_polygon_daily_bars"

    @task
    def get_s3_keys_from_conf(**kwargs) -> list[str]:
        """
        Retrieves the list of S3 keys from the DAG run configuration.
        If the list is empty, the entire DAG is skipped.
        """
        dag_run = kwargs.get("dag_run")
        s3_keys = dag_run.conf.get('s3_keys')
        if not s3_keys or not isinstance(s3_keys, list):
            raise AirflowSkipException("No S3 keys were provided. Skipping DAG run.")
        return s3_keys

    @task
    def load_minio_json_to_postgres(s3_key: str):
        """
        Reads a single JSON file from MinIO, parses it, and loads the data into Postgres.
        """
        if not isinstance(s3_key, str):
            raise TypeError(f"Expected s3_key to be a string, but got {type(s3_key)}.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        json_data_string = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
        data = json.loads(json_data_string)

        if data.get('resultsCount', 0) == 0:
            print(f"No data found in {s3_key}. Skipping load for this file.")
            return

        ticker = data.get("ticker", "UNKNOWN")
        results = data.get("results", [])
        
        df = pd.DataFrame(results)
        df['ticker'] = ticker
        df = df.rename(columns={
            'v': 'volume', 'o': 'open_price', 'c': 'close_price',
            'h': 'high_price', 'l': 'low_price', 't': 'timestamp_ms'
        })
        df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
        df = df[['date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE, ticker VARCHAR(20), open_price NUMERIC(18, 4),
            high_price NUMERIC(18, 4), low_price NUMERIC(18, 4),
            close_price NUMERIC(18, 4), volume BIGINT
        );
        """
        pg_hook.run(create_table_sql)

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                load_date = df['date'].iloc[0]
                cursor.execute(
                    f"DELETE FROM {TABLE_NAME} WHERE date = %s AND ticker = %s;",
                    (load_date, ticker)
                )
                pg_hook.insert_rows(
                    table=TABLE_NAME,
                    rows=df.to_records(index=False).tolist(),
                    target_fields=df.columns.tolist(),
                    commit_every=0,
                    conn=conn
                )
        print(f"Successfully loaded {len(df)} rows into {TABLE_NAME} for ticker {ticker}.")

    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="stocks_dbt_transform",
    )

    s3_keys = get_s3_keys_from_conf()
    load_tasks = load_minio_json_to_postgres.expand(s3_key=s3_keys)
    
    load_tasks >> trigger_dbt_dag

stocks_polygon_load_dag()
