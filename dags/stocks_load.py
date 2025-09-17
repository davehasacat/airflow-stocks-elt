import pendulum
import os
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator

@dag(
    dag_id="load_stocks_from_minio",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["loading", "minio", "postgres", "dq"],
)
def load_stocks_dag():
    """
    This DAG is event-driven. It waits for a file to appear in Minio,
    loads it into Postgres, and runs a data quality check.
    """

    # --- Configuration ---
    S3_CONN_ID = os.getenv("S3_CONN_ID", "minio_s3")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID", "postgres_dwh")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    TABLE_NAME = "alpha_vantage_daily"
    
    # --- This is the key change ---
    # The DAG now constructs the expected filename from the same environment
    # variable used by the ingestion DAG. This is more reliable than XComs.
    TICKER = os.getenv("TICKER", "GOOGL")
    s3_key = f"{TICKER}_daily.json"

    wait_for_file_in_minio = S3KeySensor(
        task_id="wait_for_file_in_minio",
        bucket_name=BUCKET_NAME,
        bucket_key=s3_key, # The sensor now waits for the predictable filename
        aws_conn_id=S3_CONN_ID,
        mode='poke',
        poke_interval=30,
        timeout=600
    )

    @task
    def load_minio_json_to_postgres():
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        json_data_string = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
        data = json.loads(json_data_string)
        time_series_data = data.get("Time Series (Daily)")
        if not time_series_data:
            raise ValueError(f"JSON file {s3_key} does not contain 'Time Series (Daily)' data.")
        df = pd.DataFrame.from_dict(time_series_data, orient='index')
        df.reset_index(inplace=True)
        df = df.rename(columns={'index': 'date', '1. open': 'open', '2. high': 'high', '3. low': 'low', '4. close': 'close', '5. volume': 'volume'})
        meta_data = data.get("Meta Data", {})
        ticker = meta_data.get("2. Symbol", "UNKNOWN")
        df['ticker'] = ticker
        df = df[['date', 'open', 'high', 'low', 'close', 'volume', 'ticker']]
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(f"CREATE TABLE IF NOT EXISTS {TABLE_NAME} (date DATE, open NUMERIC, high NUMERIC, low NUMERIC, close NUMERIC, volume BIGINT, ticker VARCHAR(10));")
        cursor.execute(f"TRUNCATE TABLE {TABLE_NAME};")
        conn.commit()
        cursor.close()
        conn.close()
        rows = list(df.itertuples(index=False, name=None))
        pg_hook.insert_rows(table=TABLE_NAME, rows=rows, target_fields=df.columns.tolist())
        print(f"Successfully loaded {len(df)} rows into {TABLE_NAME}.")

    check_table_has_rows = SQLTableCheckOperator(
        task_id="check_table_has_rows",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        checks={"row_count_check": {"check_statement": "COUNT(*) > 0"}}
    )
    
    # The 'dbt_run_models' trigger is not in this version,
    # as we are focusing on getting the ingest -> load connection working.
    # It can be added back as a final step.
    wait_for_file_in_minio >> load_minio_json_to_postgres() >> check_table_has_rows

load_stocks_dag()
