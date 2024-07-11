from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from sql.sql_create_tables import CREATE_TRANSFORMED_TABLE, CREATE_RAW_TABLE


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="init_db_dag",
    schedule_interval=None,
    max_active_runs=2,
    catchup=False,
    default_args=default_args,
) as dag:

    create_raw_table = SQLExecuteQueryOperator(
        task_id="create_raw_table", conn_id="postgres_localhost", sql=CREATE_RAW_TABLE
    )

    create_transformed_table = SQLExecuteQueryOperator(
        task_id="create_transformed_table",
        conn_id="postgres_localhost",
        sql=CREATE_TRANSFORMED_TABLE,
    )

    insert_sample_data = SQLExecuteQueryOperator(
        task_id="insert_sample_data",
        conn_id="postgres_localhost",
        sql="""
        INSERT INTO spotify_songs_raw (raw_json, processed, fetched_timestamp, played_at_timestamp) 
        VALUES (
            '{"items": [{"played_at": "2023-07-08T10:00:00Z", "track": {"name": "Sample Song", "album": {"artists": [{"name": "Sample Artist"}], "images": [{"url": ""}, {"url": "http://sample_url"}], "name": "Sample Album", "id": "sample_album_id"}, "duration_ms": 180000, "external_urls": {"spotify": "http://sample_song_link"}, "artists": [{"id": "sample_artist_id"}], "id": "sample_track_id"}}]}'
            , TRUE
            , '2023-07-08T10:00:00Z'
            , '2023-07-08T10:00:00Z'
        );
        """,
    )  # set processed to true so the sample data doesn't get included later.

    create_raw_table >> insert_sample_data
    create_transformed_table
