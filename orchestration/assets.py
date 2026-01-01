import os
import sys
from dagster import asset, AssetExecutionContext

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.pipelines.teams_pipeline import run_teams_pipeline
from ingestion.pipelines.fixtures_pipeline import run_fixtures_pipeline
from ingestion.pipelines.standings_pipeline import run_standings_pipeline
from ingestion.pipelines.scorers_pipeline import run_scorers_pipeline
from ingestion.pipelines.players_pipeline import run_players_pipeline
from ingestion.utils.duckdb_setup import get_connection


@asset(group_name="ingestion")
def teams_data(context: AssetExecutionContext):
    """Ingest teams data from API-Football."""
    context.log.info("Running teams pipeline...")
    run_teams_pipeline()
    
    # Verify table was created
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'teams'
        """).fetchone()
        if result[0] == 0:
            raise Exception("Teams table was not created in raw schema")
        context.log.info(f"Verified teams table exists")
    finally:
        conn.close()
    
    context.log.info("Teams pipeline completed.")
    return {"status": "success", "table": "teams"}


@asset(group_name="ingestion", deps=[teams_data])
def fixtures_data(context: AssetExecutionContext):
    """Ingest fixtures data from API-Football."""
    context.log.info("Running fixtures pipeline...")
    run_fixtures_pipeline()
    
    # Verify table was created
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'fixtures'
        """).fetchone()
        if result[0] == 0:
            raise Exception("Fixtures table was not created in raw schema")
        context.log.info(f"Verified fixtures table exists")
    finally:
        conn.close()
    
    context.log.info("Fixtures pipeline completed.")
    return {"status": "success", "table": "fixtures"}


@asset(group_name="ingestion", deps=[fixtures_data])
def standings_data(context: AssetExecutionContext):
    """Ingest standings data from API-Football."""
    context.log.info("Running standings pipeline...")
    run_standings_pipeline()
    
    # Verify table was created
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'standings'
        """).fetchone()
        if result[0] == 0:
            raise Exception("Standings table was not created in raw schema")
        context.log.info(f"Verified standings table exists")
    finally:
        conn.close()
    
    context.log.info("Standings pipeline completed.")
    return {"status": "success", "table": "standings"}


@asset(group_name="ingestion", deps=[standings_data])
def scorers_data(context: AssetExecutionContext):
    """Ingest top scorers data from API-Football."""
    context.log.info("Running scorers pipeline...")
    run_scorers_pipeline()
    
    # Verify table was created
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'scorers'
        """).fetchone()
        if result[0] == 0:
            raise Exception("Scorers table was not created in raw schema")
        context.log.info(f"Verified scorers table exists")
    finally:
        conn.close()
    
    context.log.info("Scorers pipeline completed.")
    return {"status": "success", "table": "scorers"}


dbt_project_dir = os.path.join(project_root, "dbt")
print(f"dbt project directory: {dbt_project_dir}")

@asset(group_name="ingestion", deps=[standings_data])
def players_data(context: AssetExecutionContext):
    """Ingest players data from API-Football."""
    context.log.info("Running players pipeline...")
    run_players_pipeline()
    
    # Verify table was created
    conn = get_connection()
    try:
        result = conn.execute("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'players'
        """).fetchone()
        if result[0] == 0:
            raise Exception("Players table was not created in raw schema")
        context.log.info(f"Verified players table exists")
    finally:
        conn.close()
    
    context.log.info("Players pipeline completed.")
    return {"status": "success", "table": "players"}


dbt_project_dir = os.path.join(project_root, "dbt")
print(f"dbt project directory: {dbt_project_dir}")

@asset(
    group_name="transformation",
    deps=[teams_data, fixtures_data, standings_data, scorers_data, players_data],
)
def dbt_transformations(context: AssetExecutionContext):
    import subprocess
    import os

    context.log.info("Running dbt transformations...")

    DBT_BIN = os.path.join(project_root, ".venv", "bin", "dbt")
    if not os.path.exists(DBT_BIN):
        raise Exception(f"dbt binary not found at {DBT_BIN}")

    if not os.path.exists(dbt_project_dir):
        raise Exception(f"dbt project directory does not exist: {dbt_project_dir}")

    original_dir = os.getcwd()
    try:
        os.chdir(dbt_project_dir)

        result = subprocess.run(
            [DBT_BIN, "run"],
            capture_output=True,
            text=True,
            check=False,
        )

        if result.returncode != 0:
            raise Exception(result.stderr or result.stdout)

        context.log.info("dbt transformations completed successfully")
        context.log.info(result.stdout)

    finally:
        os.chdir(original_dir)

    return {"status": "success"}
