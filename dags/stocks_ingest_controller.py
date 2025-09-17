from __future__ import annotations
import pendulum
from airflow.decorators import dag
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# This list defines which stocks we want to ingest.
# In a production system, this could be read from a database or a file.
TICKERS_TO_INGEST = ["AAPL", "MSFT", "GOOGL", "AMZN"]

@dag(
    dag_id="stocks_ingest_controller",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["controller", "ingestion"],
)
def stocks_ingest_controller_dag():
    """
    This DAG triggers the 'ingest_stocks' DAG for each ticker in a list.
    """
    TriggerDagRunOperator.partial(
        task_id="trigger_ingest_dag",
        trigger_dag_id="ingest_stocks",
    ).expand(
        conf=[{"ticker": ticker} for ticker in TICKERS_TO_INGEST]
    )

stocks_ingest_controller_dag()
