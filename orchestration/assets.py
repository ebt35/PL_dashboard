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
from ingestion.utils.logger import setup_logger

orchestration_logger = setup_logger("orchestration")


@asset(group_name="ingestion")
def teams_data(context: AssetExecutionContext):
    """Ingest teams data from API-Football."""
    orchestration_logger.info("Orchestration: Starting teams pipeline")
    context.log.info("Running teams pipeline...")
    run_teams_pipeline()
    orchestration_logger.info("Orchestration: Teams pipeline completed")
    context.log.info("Teams pipeline completed.")
    return {"status": "success", "table": "teams"}


@asset(group_name="ingestion", deps=[teams_data])
def fixtures_data(context: AssetExecutionContext):
    """Ingest fixtures data from API-Football."""
    orchestration_logger.info("Orchestration: Starting fixtures pipeline")
    context.log.info("Running fixtures pipeline...")
    run_fixtures_pipeline()
    orchestration_logger.info("Orchestration: Fixtures pipeline completed")
    context.log.info("Fixtures pipeline completed.")
    return {"status": "success", "table": "fixtures"}


@asset(group_name="ingestion", deps=[fixtures_data])
def standings_data(context: AssetExecutionContext):
    """Ingest standings data from API-Football."""
    orchestration_logger.info("Orchestration: Starting standings pipeline")
    context.log.info("Running standings pipeline...")
    run_standings_pipeline()
    orchestration_logger.info("Orchestration: Standings pipeline completed")
    context.log.info("Standings pipeline completed.")
    return {"status": "success", "table": "standings"}


@asset(group_name="ingestion", deps=[standings_data])
def scorers_data(context: AssetExecutionContext):
    """Ingest top scorers data from API-Football."""
    orchestration_logger.info("Orchestration: Starting scorers pipeline")
    context.log.info("Running scorers pipeline...")
    run_scorers_pipeline()
    orchestration_logger.info("Orchestration: Scorers pipeline completed")
    context.log.info("Scorers pipeline completed.")
    return {"status": "success", "table": "scorers"}


dbt_project_dir = os.path.join(project_root, "dbt")
print("\n \n")
print(dbt_project_dir)

@asset(
    group_name="transformation",
    deps=[teams_data, fixtures_data, standings_data, scorers_data],
)
def dbt_transformations(context: AssetExecutionContext):
    """Run dbt transformations on ingested data."""
    import subprocess
    
    orchestration_logger.info("Orchestration: Starting dbt transformations")
    context.log.info("Running dbt transformations...")
    
    # Change to dbt directory and run dbt
    original_dir = os.getcwd()
    try:
        os.chdir(dbt_project_dir)
        result = subprocess.run(
            ["uv","run","dbt", "run"],
            capture_output=True,
            text=True,
            check=False
        )
        
        if result.returncode == 0:
            orchestration_logger.info("Orchestration: dbt transformations completed successfully")
            orchestration_logger.info(f"dbt output: {result.stdout}")
            context.log.info("dbt transformations completed successfully.")
            context.log.info(result.stdout)
        else:
            orchestration_logger.error(f"Orchestration: dbt run failed: {result.stderr}")
            context.log.error(f"dbt run failed: {result.stderr}")
            raise Exception(f"dbt run failed: {result.stderr}")
    finally:
        os.chdir(original_dir)
    
    return {"status": "success", "transformations": "completed"}