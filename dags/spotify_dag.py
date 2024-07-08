from airflow import DAG
from airflow.operators.python import PythonOperator, variables
from datetime import datetime, timedelta
from spotify_etl import fetch_spotify_data

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


dag = DAG("spotify_dag", default_args=default_args, description="test spotify dag")

run_etl = PythonOperator(
    task_id="complete_spotify_etl", python_callable=fetch_spotify_data(), dag=dag
)


run_etl
