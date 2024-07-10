# from sqlalchemy import create_engine
# from auth import HOST, DB_NAME, PORT, PG_PASSWORD, PG_USER
from airflow.providers.postgres.hooks.postgres import PostgresHook


class PgConnect:
    def __init__(self, conn_id="postgres_localhost"):
        self.conn_id = conn_id
        self.hook = PostgresHook(postgres_conn_id=self.conn_id)
        self.connection = None

    def __enter__(self):
        self.connection = self.hook.get_conn()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.close()
