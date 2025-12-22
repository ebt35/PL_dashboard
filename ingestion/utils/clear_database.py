import os
import sys
import duckdb

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.config import DUCKDB_PATH

def clear_database():
    """Clear all data from the database while keeping schema structure."""
    db_path = os.path.abspath(DUCKDB_PATH)
    
    if not os.path.exists(db_path):
        print("Database file does not exist. Nothing to clear.")
        return
    
    conn = duckdb.connect(db_path)
    
    try:
        print("Clearing database...")
        
        # Get all schemas
        schemas = conn.execute("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name NOT IN ('information_schema', 'pg_catalog', 'main')
        """).fetchall()
        
        for schema in schemas:
            schema_name = schema[0]
            print(f"\nClearing schema: {schema_name}")
            
            # Get all tables in schema
            tables = conn.execute(f"""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = '{schema_name}'
            """).fetchall()
            
            for table in tables:
                table_name = table[0]
                try:
                    conn.execute(f"DROP TABLE IF EXISTS {schema_name}.{table_name}")
                    print(f"  ✓ Dropped table: {schema_name}.{table_name}")
                except Exception as e:
                    print(f"  ⚠ Could not drop {schema_name}.{table_name}: {e}")
        
        # Drop schemas (except raw which will be recreated)
        for schema in schemas:
            schema_name = schema[0]
            if schema_name not in ['raw']:
                try:
                    conn.execute(f"DROP SCHEMA IF EXISTS {schema_name} CASCADE")
                    print(f"  ✓ Dropped schema: {schema_name}")
                except Exception as e:
                    print(f"  ⚠ Could not drop schema {schema_name}: {e}")
        
        print("\n✓ Database cleared successfully!")
        print("You can now run the pipeline from scratch.")
        
    finally:
        conn.close()

if __name__ == "__main__":
    clear_database()

