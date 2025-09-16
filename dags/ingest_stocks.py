import pendulum
import os
import requests
import json
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

"""
this DAG is designed to pull in data for only one ticker.
this is to ensure our tech stack is working as expected.
data transformations will be completed and tested, then i'll be
incorporating a for loop to pull in data for all tickers
"""
@dag(
    dag_id="ingest_stock_data",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["ingestion", "alphavantage"],
)
def ingest_stock_data_dag():
    """
    This DAG fetches daily stock data from Alpha Vantage, saves it as a JSON
    file, and then uploads that file to Minio S3 storage.
    """

    @task
    def fetch_and_save_daily_data() -> str:
        """
        Fetches stock data and saves it to a local JSON file.
        Returns the path to the created file.
        """
        TICKER = "GOOGLE"
        # Using Airflow's temporary directory for intermediate files
        output_path = f"/tmp/{TICKER}_daily.json"

        api_key = os.getenv("ALPHA_ADVANTAGE_API_KEY")
        if not api_key:
            raise ValueError("ALPHA_ADVANTAGE_API_KEY environment variable not set.")

        url = (
            "https://www.alphavantage.co/query?"
            "function=TIME_SERIES_DAILY_ADJUSTED&"
            f"symbol={TICKER}&"
            "outputsize=full&"
            f"apikey={api_key}"
        )
        
        print(f"Fetching data for {TICKER} from Alpha Vantage...")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        print("Data fetched successfully.")

        print(f"Saving data temporarily to {output_path}...")
        with open(output_path, "w") as f:
            json.dump(data, f)
        print("Data saved successfully.")

        return output_path

    @task
    def upload_to_minio(local_file_path: str):
        """
        Uploads the given file to a specified bucket in Minio.
        """
        # --- Configuration ---
        S3_CONN_ID = "minio_s3"
        BUCKET_NAME = "test"
        # Use the filename from the local path as the S3 key
        s3_key = os.path.basename(local_file_path)

        print(f"Uploading {local_file_path} to Minio bucket '{BUCKET_NAME}' as '{s3_key}'...")

        # Use the S3Hook to interact with Minio
        s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
        s3_hook.load_file(
            filename=local_file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True  # Overwrite the file if it already exists
        )
        print("Upload successful.")

    # --- Task Chaining ---
    # The output of the first task (the file path) is passed to the second task
    local_path = fetch_and_save_daily_data()
    upload_to_minio(local_file_path=local_path)

ingest_stock_data_dag()
