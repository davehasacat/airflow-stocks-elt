import pendulum
from airflow.decorators import dag
from cosmos import DbtTaskGroup, ProjectConfig, ProfileConfig

@dag(
    dag_id="dbt_run_models",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None, # This DAG will be triggered by another process
    catchup=False,
    tags=["dbt", "transformation"],
)
def dbt_run_models_dag():
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
    )

dbt_run_models_dag()
