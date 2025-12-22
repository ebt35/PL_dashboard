import duckdb
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.config import DUCKDB_PATH

def init_database():
    import time
    db_path = os.path.abspath(DUCKDB_PATH)
    db_dir = os.path.dirname(db_path)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)
    
    # Retry logic for file lock issues
    max_retries = 10
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            conn = duckdb.connect(db_path)
            try:
                conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
            finally:
                conn.close()
            return db_path
        except Exception as e:
            error_str = str(e)
            is_lock_error = (
                "being used by another process" in error_str or
                "Cannot open file" in error_str or
                "IO Error" in error_str
            )
            if is_lock_error and attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            raise

def get_connection():
    import time
    db_path = os.path.abspath(DUCKDB_PATH)
    
    # Retry logic for file lock issues
    max_retries = 10
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            return duckdb.connect(db_path)
        except Exception as e:
            error_str = str(e)
            is_lock_error = (
                "being used by another process" in error_str or
                "Cannot open file" in error_str or
                "IO Error" in error_str
            )
            if is_lock_error and attempt < max_retries - 1:
                time.sleep(retry_delay * (attempt + 1))
                continue
            raise

def execute_query(query: str, params=None):
    conn = get_connection()
    try:
        if params:
            result = conn.execute(query, params).fetchall()
        else:
            result = conn.execute(query).fetchall()
        return result
    finally:
        conn.close()

def get_table_info(schema: str = "raw"):
    conn = get_connection()
    try:
        tables = conn.execute(f"""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}'
        """).fetchall()
        return [table[0] for table in tables]
    finally:
        conn.close()

def get_table_row_count(table_name: str, schema: str = "raw"):
    conn = get_connection()
    try:
        result = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table_name}").fetchone()
        return result[0] if result else 0
    except Exception:
        return 0
    finally:
        conn.close()

