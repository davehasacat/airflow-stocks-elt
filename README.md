Airflow Stock Exchange ETL
===========================

This project is a complete, containerized ETL (Extract, Transform, Load) environment designed for processing stock data. It uses a modern data stack to orchestrate data pipelines, manage transformations, and store data.

Tech Stack

This project is built with the following tools and technologies:

Orchestration: Apache Airflow
Used to schedule, execute, and monitor data pipelines (DAGs).

Containerization: Docker
Used to create a consistent, isolated, and reproducible environment for all services.

Development CLI: Astro CLI
Used to streamline local development, testing, and deployment of the Airflow environment.

Object Storage: Minio
Serves as an S3-compatible object storage solution for raw data, files, and other artifacts.

Data Warehouse: Postgres
Acts as the central data warehouse where transformed and structured data is stored for analysis.

Transformation: dbt (Data Build Tool)
Used to transform raw data in the warehouse into clean, reliable, and analytics-ready datasets using SQL.

Getting Started

Start the environment:

    Bash
    
    astro dev start

Create the Minio Bucket:

- Navigate to the Minio console at http://localhost:9001.
- Log in with the credentials from your .env file.
- Create a new bucket named test.

Run the Connection Test:

- Navigate to the Airflow UI at http://localhost:8080.
- Un-pause and trigger the full_stack_connection_test DAG to verify all services are connected.

Documentation and Links

[insert docs]
