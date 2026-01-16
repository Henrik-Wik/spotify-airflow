from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.dbt.operators.dbt import DbtRunOperator
from airflow.providers.dbt.operators.dbt import DbtTestOperator
from datetime import datetime, timedelta
from tasks.fetch_spotify_data import FetchSpotifyData


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# instantiate class to call function
def fetch_data_callable() -> None:
    fetch_data = FetchSpotifyData()
    fetch_data.load_data()


# dag for the main etl process.
with DAG(
    dag_id="etl_dag",
    schedule_interval="0 * * * *",  # runs hourly
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetching_data",
        python_callable=fetch_data_callable,
        dag=dag,
    )

# Run dbt transformations
    run_dbt_models = DbtRunOperator(
        task_id="run_dbt_models",
        project_dir="/opt/airflow/spotify_airflow_dbt",
        profiles_dir="/opt/airflow/spotify_airflow_dbt",
        target="dev",
    )
    
    # Run dbt tests
    test_dbt_models = DbtTestOperator(
        task_id="test_dbt_models",
        project_dir="/opt/airflow/spotify_airflow_dbt",
        profiles_dir="/opt/airflow/spotify_airflow_dbt",
        target="dev",
    )
    
    # Mark records as transformed
    mark_transformed = SQLExecuteQueryOperator(
        task_id="mark_transformed",
        conn_id="postgres_localhost",
        sql="""
            UPDATE recently_played_raw
            SET transformed = TRUE
            WHERE transformed = FALSE;
        """,
    )

fetch_data >> run_dbt_models >> test_dbt_models >> mark_transformed
