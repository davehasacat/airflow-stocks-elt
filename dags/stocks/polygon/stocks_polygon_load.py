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
    # Define connection IDs and S3 bucket name
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    TABLE_NAME = "source_stocks_polygon_daily_bars"

    # Task to get S3 keys from the DAG run config and split them into batches
    @task
    def batch_s3_keys_from_conf(**kwargs) -> list[str]:
        dag_run = kwargs.get("dag_run")
        s3_keys = dag_run.conf.get('s3_keys')
        
        # Skip DAG run if no S3 keys are provided
        if not s3_keys or not isinstance(s3_keys, list):
            raise AirflowSkipException("No S3 keys were provided. Skipping DAG run.")
            
        # Split the S3 keys into batches and save each batch to a new S3 file
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        batch_size = 500  # A manageable number of keys per batch
        batch_file_keys = []
        for i in range(0, len(s3_keys), batch_size):
            batch = s3_keys[i:i + batch_size]
            batch_string = "\n".join(batch)
            batch_file_key = f"load_batches/s3_keys_batch_{i // batch_size + 1}.txt"
            s3_hook.load_string(string_data=batch_string, key=batch_file_key, bucket_name=BUCKET_NAME, replace=True)
            batch_file_keys.append(batch_file_key)
            
        # Return the list of S3 keys for the batches, which is small enough for XComs
        return batch_file_keys

    # Task to load a batch of JSON files from S3 into Postgres
    @task
    def load_s3_key_batch_to_postgres(batch_s3_key: str):
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        # Read the list of S3 keys for this batch from the batch file
        keys_string = s3_hook.read_key(key=batch_s3_key, bucket_name=BUCKET_NAME)
        s3_keys_in_batch = keys_string.splitlines()

        # Iterate over each S3 key in the batch and load its data
        for s3_key in s3_keys_in_batch:
            json_data_string = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
            data = json.loads(json_data_string)

            # Skip if the file contains no data
            if data.get('resultsCount', 0) == 0:
                print(f"No data found in {s3_key}. Skipping load for this file.")
                continue

            # Process and transform the data using pandas
            ticker = data.get("ticker", "UNKNOWN")
            results = data.get("results", [])
            df = pd.DataFrame(results)
            df['ticker'] = ticker
            df = df.rename(columns={'v': 'volume', 'o': 'open_price', 'c': 'close_price', 'h': 'high_price', 'l': 'low_price', 't': 'timestamp_ms'})
            df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
            df = df[['date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
            
            # Create table if it doesn't exist
            create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                date DATE, ticker VARCHAR(20), open_price NUMERIC(18, 4),
                high_price NUMERIC(18, 4), low_price NUMERIC(18, 4),
                close_price NUMERIC(18, 4), volume BIGINT
            );
            """
            pg_hook.run(create_table_sql)

            # Insert data into Postgres, ensuring idempotency
            with pg_hook.get_conn() as conn:
                with conn.cursor() as cursor:
                    load_date = df['date'].iloc[0]
                    cursor.execute(f"DELETE FROM {TABLE_NAME} WHERE date = %s AND ticker = %s;", (load_date, ticker))
                    pg_hook.insert_rows(
                        table=TABLE_NAME,
                        rows=df.to_records(index=False).tolist(),
                        target_fields=df.columns.tolist(),
                        commit_every=1000, # Commit in chunks for better performance
                        conn=conn
                    )
            print(f"Successfully loaded {len(df)} rows into {TABLE_NAME} for ticker {ticker}.")

    # Define the final task to trigger the dbt transformation DAG
    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="stocks_dbt_transform",
    )

    # Define the DAG's task dependencies
    s3_key_batches = batch_s3_keys_from_conf()
    load_tasks = load_s3_key_batch_to_postgres.expand(batch_s3_key=s3_key_batches)
    
    # Set the dependency for the final trigger task
    load_tasks >> trigger_dbt_dag

stocks_polygon_load_dag()
