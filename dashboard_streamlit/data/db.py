import duckdb
import os

DUCKDB_PATH = os.getenv(
    "DUCKDB_PATH",
    os.path.join("duckdb", "football.duckdb")
)

def get_db_connection():
    return duckdb.connect(DUCKDB_PATH, read_only=True)
