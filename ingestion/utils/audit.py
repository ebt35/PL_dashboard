from datetime import datetime
from typing import Optional
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.utils.duckdb_setup import get_connection, init_database

def init_audit_table():
    import time
    max_retries = 10
    retry_delay = 1.0
    
    for attempt in range(max_retries):
        try:
            init_database()
            conn = get_connection()
            try:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.ingestion_audit (
                        id INTEGER PRIMARY KEY,
                        source_endpoint VARCHAR,
                        target_table VARCHAR,
                        rows_loaded INTEGER,
                        ingestion_timestamp TIMESTAMP,
                        status VARCHAR
                    )
                """)
            finally:
                conn.close()
            return
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

def log_ingestion(source_endpoint: str, target_table: str, rows_loaded: int, status: str = "success"):
    conn = get_connection()
    
    max_id = conn.execute("SELECT COALESCE(MAX(id), 0) FROM raw.ingestion_audit").fetchone()[0]
    new_id = max_id + 1
    
    conn.execute("""
        INSERT INTO raw.ingestion_audit 
        (id, source_endpoint, target_table, rows_loaded, ingestion_timestamp, status)
        VALUES (?, ?, ?, ?, ?, ?)
    """, [new_id, source_endpoint, target_table, rows_loaded, datetime.now(), status])
    
    conn.close()

def is_first_load(table_name: str) -> bool:
    conn = get_connection()
    
    result = conn.execute("""
        SELECT COUNT(*) as count 
        FROM raw.ingestion_audit 
        WHERE target_table = ? AND status = 'success'
    """, [table_name]).fetchone()
    
    conn.close()
    return result[0] == 0

