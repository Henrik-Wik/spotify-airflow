from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from fetch_data import FetchSpotifyData
from sql_transform_data import TRANSFORM_AND_UPDATE_DATA


def fetch_data_callable():
    fetch_data = FetchSpotifyData()
    fetch_data.get_data()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="etl_dag",
    schedule_interval="0 0 * * *",
    max_active_runs=2,
    catchup=False,
    default_args=default_args,
) as dag:

    fetch_data = PythonOperator(
        task_id="fetching_data",
        python_callable=fetch_data_callable,
        dag=dag,
    )

    transform_and_update_data = SQLExecuteQueryOperator(
        task_id="transforming_data",
        conn_id="postgres_localhost",
        sql=TRANSFORM_AND_UPDATE_DATA,
    )


fetch_data >> transform_and_update_data
