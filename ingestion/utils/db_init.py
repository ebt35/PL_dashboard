from ingestion.utils.duckdb_setup import init_database
from ingestion.utils.audit import init_audit_table

def initialize_database():
    print("Initializing DuckDB database...")
    db_path = init_database()
    print(f"Database initialized at: {db_path}")
    
    print("Creating audit table...")
    init_audit_table()
    print("Audit table created.")
    
    print("Database setup complete!")

if __name__ == "__main__":
    initialize_database()

