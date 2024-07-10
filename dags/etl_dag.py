from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from fetch_data import FetchSpotifyData


def fetch_data_callable():
    fetch_data = FetchSpotifyData()
    fetch_data.get_data()


# def transform_data_callable():
#     fetch_data = FetchSpotifyData()
#     fetch_data.transform_data()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="spotify_dag",
    schedule_interval="0 * * * *",
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetching_data",
        python_callable=fetch_data_callable,
        dag=dag,
    )

    # transform_data = PythonOperator(
    #     task_id="transforming_data",
    #     python_callable=transform_data_callable,
    #     dag=dag,
    # )


fetch_data
