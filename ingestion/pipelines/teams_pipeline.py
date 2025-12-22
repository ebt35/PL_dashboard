import dlt
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.sources.api_football import APIFootballClient
from ingestion.utils.audit import init_audit_table, log_ingestion
from ingestion.utils.logger import setup_logger
from ingestion.config import LEAGUE_ID, SEASON, DUCKDB_PATH

logger = setup_logger("teams_pipeline")

def flatten_team(team_data):
    team = team_data.get("team", {})
    venue = team_data.get("venue", {})
    
    return {
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "team_code": team.get("code"),
        "team_country": team.get("country"),
        "team_founded": team.get("founded"),
        "team_national": team.get("national"),
        "team_logo": team.get("logo"),
        "venue_id": venue.get("id"),
        "venue_name": venue.get("name"),
        "venue_address": venue.get("address"),
        "venue_city": venue.get("city"),
        "venue_capacity": venue.get("capacity"),
        "venue_surface": venue.get("surface"),
        "venue_image": venue.get("image")
    }

@dlt.resource(name="teams", write_disposition="replace")
def teams_resource():
    client = APIFootballClient()
    teams_data = client.get_teams(LEAGUE_ID, SEASON)
    
    for team in teams_data:
        yield flatten_team(team)

def run_teams_pipeline():
    logger.info("Starting teams pipeline")
    init_audit_table()
    
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="teams_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    try:
        logger.info("Fetching teams data from API")
        teams_gen = teams_resource()
        teams_list = list(teams_gen)
        rows_count = len(teams_list)
        logger.info(f"Fetched {rows_count} teams")
        
        logger.info("Loading teams data to DuckDB")
        info = pipeline.run(teams_resource())
        
        logger.info(f"Successfully loaded {rows_count} teams to raw.teams")
        
        log_ingestion(
            source_endpoint="teams",
            target_table="teams",
            rows_loaded=rows_count,
            status="success"
        )
        
        return info
    except Exception as e:
        logger.error(f"Teams pipeline failed: {str(e)}")
        log_ingestion(
            source_endpoint="teams",
            target_table="teams",
            rows_loaded=0,
            status=f"failed: {str(e)}"
        )
        raise

if __name__ == "__main__":
    run_teams_pipeline()

