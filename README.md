# Airflow Stocks ELT Project

This project is a complete, containerized ELT (Extract, Load, Transform) environment designed for processing stock data. It uses a modern data stack to orchestrate data pipelines, manage transformations, and store data, providing a robust foundation for financial analysis and backtesting.

## Version: 1.0 (Proof of Concept)

This version of the project establishes a complete, end-to-end pipeline for a single stock ticker. It serves as a proof of concept for the architecture and technology stack. Future versions will focus on scaling the pipeline to handle multiple tickers, implementing more complex transformations, and adding advanced data quality checks and monitoring.

---

## Pipeline Architecture

The ELT process is orchestrated by three modular and event-driven Airflow DAGs that form a seamless, automated workflow.

1. **`ingest_stocks`**: Extracts daily data from the Alpha Vantage API, lands the raw JSON file in Minio object storage, and then automatically triggers the `load_stocks_from_minio` DAG.
2. **`load_stocks_from_minio`**: Waits for the file to appear in Minio, parses the raw JSON, loads it into a raw table in the Postgres data warehouse, runs a data quality check, and then automatically triggers the `dbt_run_models` DAG.
3. **`dbt_run_models`**: Runs the dbt models to transform the raw data into clean, analytics-ready tables (views).

### Proof of Success

The screenshot below shows a successful, end-to-end run of the entire orchestrated pipeline in the Airflow UI, demonstrating the successful execution of all four DAGs.

#### Successful Airflow Pipeline Run
<img width="1848" height="1080" alt="Capture" src="https://github.com/user-attachments/assets/46bcf2fb-f9e6-4fb6-a561-b95d3d54463e" />

---

## Tech Stack

* **Orchestration**: **Apache Airflow**
  * Used to schedule, execute, and monitor the data pipelines (DAGs).
* **Containerization**: **Docker**
  * Used to create a consistent, isolated, and reproducible environment for all services.
* **Development CLI**: **Astro CLI**
  * Used to streamline local development and testing of the Airflow environment.
* **Object Storage**: **Minio**
  * Serves as an S3-compatible object storage solution for raw data files.
* **Data Warehouse**: **Postgres**
  * Acts as the central data warehouse where both raw and transformed data is stored.
* **Transformation**: **dbt (Data Build Tool)**
  * Used to transform raw data in the warehouse into clean, reliable, and analytics-ready datasets using SQL.

---

## Getting Started

### Prerequisites

* Docker Desktop
* Astro CLI
* An Alpha Vantage API Key

### Configuration

1. **Environment Variables**: Create a file named `.env` in the project root. Copy the contents of `.env.example` (if you've created one) into it and fill in your `ALPHA_ADVANTAGE_API_KEY`. The rest of the variables are pre-configured for the local environment.
2. **dbt Profile**: The `dbt/profiles.yml` file is configured to read credentials from the `.env` file. No changes are needed.

### Running the Project

1. **Start the environment** with a single command from your project's root directory:

    ```bash
    astro dev start
    ```

2. **Create the Minio Bucket**:
    * Navigate to the Minio console at `http://localhost:9001`.
    * Log in with the credentials from your `.env` file (`minioadmin`/`minioadmin`).
    * Create a new bucket named `test`.

3. **Run the Full Pipeline**:
    * Navigate to the Airflow UI at `http://localhost:8080`.
    * Log in with `admin` / `admin`.
    * Un-pause the `ingest_stocks` DAG and trigger it with a manual run. This will kick off the entire automated pipeline.

---

## Future Work & Scalability

This proof of concept serves as a strong foundation. The next steps for expanding this project include:

 [ ] **Scale Ingestion with Airflow Params**: Parameterize the DAGs to accept a list of tickers, allowing for the ingestion of hundreds of stocks in parallel using Dynamic Task Mapping.
 [ ] **Implement an Incremental Loading Strategy**: Shift from a "truncate and load" pattern to an "append and merge" pattern to preserve historical data.
 [ ] **Build Out dbt Marts Layer**: Create final analytical tables, such as weekly/monthly aggregations and technical indicators (e.g., moving averages).

## Documentation

For more detailed information on the tools and technologies used in this project, please refer to their official documentation:

* **[Apache Airflow Documentation](https://airflow.apache.org/docs/)**
* **[Astro CLI Documentation](https://www.astronomer.io/docs/astro/cli/overview)**
* **[Docker Documentation](https://docs.docker.com/)**
* **[Minio Documentation](https://docs.min.io/)**
* **[PostgreSQL Documentation](https://www.postgresql.org/docs/)**
* **[dbt Documentation](https://docs.getdbt.com/)**
