import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig, RenderConfig

# Best Practice: Define default arguments for all tasks in the DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1)
}

@dag(
    dag_id="stocks_dbt_polygon_transform",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,  # This DAG is triggered by the 'stocks_polygon_load' DAG
    catchup=False,
    tags=["dbt", "transformation", "polygon"],
    default_args=default_args,
    doc_md="""
    ### DBT Transformation DAG for Polygon Stock Data
    
    This DAG orchestrates the transformation of raw Polygon.io stock data using dbt.
    It is triggered after the `stocks_polygon_load` DAG successfully completes.
    
    The `DbtTaskGroup` uses the `dbt build` command, which will:
    - **Run models**: To create the staging and mart tables.
    - **Run tests**: To ensure data quality and integrity.
    - **Run seeds**: To load any static data required by the models.
    """
)
def stocks_dbt_polygon_transform_dag():
    """
    This DAG uses the DbtTaskGroup to run the dbt project.
    """
    
    # Define the configurations for the dbt project and profile
    project_config = ProjectConfig(dbt_project_path="/usr/local/airflow/dbt")
    profile_config = ProfileConfig(
        profile_name="stock_data_backtesting",
        target_name="dev",
        profiles_yml_filepath="/usr/local/airflow/dbt/profiles.yml",
    )

    # Use DbtTaskGroup to execute the 'dbt build' command
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_build_tasks",
        project_config=project_config,
        profile_config=profile_config,
        operator_args={
            "install_deps": True,  # Ensures dbt dependencies are installed before running
        },
        # As your project grows, you can use `render_config` to select specific models to run.
        # For example, to run only models with the 'daily' tag:
        # render_config=RenderConfig(select=["+tag:daily"])
    )

stocks_dbt_polygon_transform_dag()
