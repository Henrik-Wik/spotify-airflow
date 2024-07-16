from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from tasks.sql.sql_create_tables import (
    CREATE_ARTISTS_RAW_TABLE,
    CREATE_AUDIO_FEATURES_RAW_TABLE,
    CREATE_RECENTLY_PLAYED_RAW_TABLE,
    CREATE_SPOTIFY_DATA_TRANSFORMED_TABLE,
)


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2023, 7, 8),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# dag for initializing db with tables and sample data.
with DAG(
    dag_id="init_db_dag",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:

    create_recently_played_raw_table = SQLExecuteQueryOperator(
        task_id="create_recently_played_raw_table",
        conn_id="postgres_localhost",
        sql=CREATE_RECENTLY_PLAYED_RAW_TABLE,
    )
    create_artists_raw_table = SQLExecuteQueryOperator(
        task_id="create_artists_raw_table",
        conn_id="postgres_localhost",
        sql=CREATE_ARTISTS_RAW_TABLE,
    )
    create_audio_features_raw_table = SQLExecuteQueryOperator(
        task_id="create_audio_features_raw_table",
        conn_id="postgres_localhost",
        sql=CREATE_AUDIO_FEATURES_RAW_TABLE,
    )
    create_spotify_data_transformed_table = SQLExecuteQueryOperator(
        task_id="create_spotify_data_transformed_table",
        conn_id="postgres_localhost",
        sql=CREATE_SPOTIFY_DATA_TRANSFORMED_TABLE,
    )
    create_recently_played_raw_table
    create_artists_raw_table
    create_audio_features_raw_table
    create_spotify_data_transformed_table
