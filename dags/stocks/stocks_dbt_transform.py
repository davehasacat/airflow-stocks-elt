import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

# Best Practice: Define default arguments for all tasks in the DAG
default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=1)
}

@dag(
    dag_id="stocks_dbt_transform",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # This DAG will be triggered by another process
    catchup=False,
    tags=["dbt", "transformation"],
    default_args=default_args # Apply the default args to the DAG
)
def stocks_dbt_transform_dag():
    """
    This DAG uses the DbtTaskGroup to run all models in the dbt project.
    """
    dbt_task_group = DbtTaskGroup(
        group_id="dbt_tasks",
        project_config=ProjectConfig(
            dbt_project_path="/usr/local/airflow/dbt",
        ),
        profile_config=ProfileConfig(
            profile_name="stock_data_backtesting",
            target_name="dev",
            profiles_yml_filepath="/usr/local/airflow/dbt/profiles.yml",
        ),
        operator_args={
            "install_deps": True, # Ensures dbt dependencies are installed
        },
        # Best Practice: You can also apply default_args directly to the TaskGroup
        # if you want different settings than the rest of the DAG.
        # default_args=default_args 
    )

stocks_dbt_transform_dag()
