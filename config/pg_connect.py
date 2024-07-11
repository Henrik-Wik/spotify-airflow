from contextlib import AbstractContextManager
from airflow.providers.postgres.hooks.postgres import PostgresHook
from typing import Optional
from psycopg2.extensions import connection


class PgConnect(AbstractContextManager):
    def __init__(self, conn_id: str = "postgres_localhost"):
        self.conn_id: str = conn_id
        self.hook: PostgresHook = PostgresHook(postgres_conn_id=self.conn_id)
        self.connection: Optional[connection] = None

    def __enter__(self) -> connection:
        self.connection = self.hook.get_conn()
        return self.connection

    def __exit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        if self.connection:
            self.connection.close()
