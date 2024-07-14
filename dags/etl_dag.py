from airflow import DAG
from airflow.operators.python import PythonOperator
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

fetch_data
