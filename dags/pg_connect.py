from sqlalchemy import create_engine
from auth import HOST, DB_NAME, PORT, PG_PASSWORD, PG_USER


class PgConnect:
    def __init__(self):
        self.host = HOST
        self.db_name = DB_NAME
        self.port = PORT
        self.user = PG_USER
        self.password = PG_PASSWORD
        self.engine = None

    def __enter__(self):
        self.engine = create_engine(
            f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db_name}"
        )
        return self.engine

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.engine:
            self.engine.dispose()


if __name__ == "__main__":
    # Example usage
    with PgConnect() as engine:
        if engine:
            try:
                # Example query
                result = engine.execute("SELECT * FROM your_table")
                for row in result:
                    print(row)
            except Exception as e:
                print(f"Error executing query: {e}")
