from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="init_db_dag",
    schedule_interval=None,
    max_active_runs=1,
    catchup=False,
    default_args=default_args,
) as dag:

    create_raw_table = PostgresOperator(
        task_id="create_tables",
        postgres_conn_id="postgres_localhost",
        sql="""
        CREATE TABLE IF NOT EXISTS spotify_songs_raw (
            id SERIAL PRIMARY KEY,
            raw_json JSONB,
            processed BOOLEAN DEFAULT FALSE,
            played_at_utc TIMESTAMPTZ
        );
        """,
    )

    insert_sample_data = PostgresOperator(
        task_id="insert_sample_data",
        postgres_conn_id="postgres_localhost",
        sql="""
        INSERT INTO spotify_songs_raw (raw_json, played_at_utc) VALUES
        ('{"items": [{"played_at": "2023-07-08T10:00:00Z", "track": {"name": "Sample Song", "album": {"artists": [{"name": "Sample Artist"}], "images": [{"url": ""}, {"url": "http://sample_url"}], "name": "Sample Album", "id": "sample_album_id"}, "duration_ms": 180000, "external_urls": {"spotify": "http://sample_song_link"}, "artists": [{"id": "sample_artist_id"}], "id": "sample_track_id"}}]}', '2023-07-08T10:00:00Z');
        """,
    )

    create_raw_table >> insert_sample_data
