from __future__ import annotations
import pendulum
import os
import json
import pandas as pd
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLTableCheckOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

@dag(
    dag_id="stocks_polygon_load",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["loading", "polygon", "postgres", "dq"],
)
def stocks_polygon_load_dag():
    S3_CONN_ID = os.getenv("S3_CONN_ID")
    POSTGRES_CONN_ID = os.getenv("POSTGRES_CONN_ID")
    BUCKET_NAME = os.getenv("BUCKET_NAME", "test")
    TABLE_NAME = "stg_polygon__stock_price_daily"

    @task
    def load_minio_json_to_postgres(**kwargs):
        dag_run = kwargs.get("dag_run")
        s3_key = dag_run.conf.get('s3_key')
        if not s3_key:
            raise ValueError("S3 key was not provided in the DAG run configuration.")

        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        json_data_string = s3_hook.read_key(key=s3_key, bucket_name=BUCKET_NAME)
        data = json.loads(json_data_string)

        if data.get('resultsCount', 0) == 0:
            print(f"No data found for {s3_key}. This may be a market holiday. Skipping.")
            return

        ticker = data.get("ticker", "UNKNOWN")
        results = data.get("results", [])
        
        df = pd.DataFrame(results)
        df['ticker'] = ticker

        df = df.rename(columns={
            'v': 'volume',
            'o': 'open_price',
            'c': 'close_price',
            'h': 'high_price',
            'l': 'low_price',
            't': 'timestamp_ms'
        })

        df['date'] = pd.to_datetime(df['timestamp_ms'], unit='ms').dt.date
        df = df[['date', 'ticker', 'open_price', 'high_price', 'low_price', 'close_price', 'volume']]
        
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        
        create_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            date DATE,
            ticker VARCHAR(20),
            open_price NUMERIC(18, 4),
            high_price NUMERIC(18, 4),
            low_price NUMERIC(18, 4),
            close_price NUMERIC(18, 4),
            volume BIGINT
        );
        """
        pg_hook.run(create_table_sql)

        pg_hook.run(
            f"DELETE FROM {TABLE_NAME} WHERE date = %s AND ticker = %s;",
            parameters=(df['date'].iloc[0], ticker)
        )
        
        pg_hook.insert_rows(
            table=TABLE_NAME,
            rows=df.to_records(index=False).tolist(),
            target_fields=df.columns.tolist()
        )

    check_table_has_rows = SQLTableCheckOperator(
        task_id="check_table_has_rows",
        conn_id=POSTGRES_CONN_ID,
        table=TABLE_NAME,
        checks={"row_count_check": {"check_statement": f"COUNT(*) > 0"}}
    )
    
    trigger_dbt_dag = TriggerDagRunOperator(
        task_id="trigger_dbt_dag",
        trigger_dag_id="stocks_dbt_transform",
    )

    load_minio_json_to_postgres() >> check_table_has_rows >> trigger_dbt_dag

stocks_polygon_load_dag()
