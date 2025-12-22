import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.pipelines.teams_pipeline import run_teams_pipeline
from ingestion.pipelines.fixtures_pipeline import run_fixtures_pipeline
from ingestion.pipelines.standings_pipeline import run_standings_pipeline
from ingestion.pipelines.scorers_pipeline import run_scorers_pipeline
from ingestion.utils.logger import setup_logger

logger = setup_logger("run_all")

def run_all_pipelines():
    logger.info("=" * 60)
    logger.info("Starting all ingestion pipelines")
    logger.info("=" * 60)
    
    try:
        logger.info("Running teams pipeline...")
        run_teams_pipeline()
        logger.info("Teams pipeline completed.")
        
        logger.info("Running fixtures pipeline...")
        run_fixtures_pipeline()
        logger.info("Fixtures pipeline completed.")
        
        logger.info("Running standings pipeline...")
        run_standings_pipeline()
        logger.info("Standings pipeline completed.")
        
        logger.info("Running scorers pipeline...")
        run_scorers_pipeline()
        logger.info("Scorers pipeline completed.")
        
        logger.info("=" * 60)
        logger.info("All pipelines completed successfully!")
        logger.info("=" * 60)
    except Exception as e:
        logger.error(f"Pipeline execution failed: {str(e)}")
        raise

if __name__ == "__main__":
    run_all_pipelines()

