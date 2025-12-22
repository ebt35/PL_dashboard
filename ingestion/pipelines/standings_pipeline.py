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

logger = setup_logger("standings_pipeline")

def flatten_standing(standing_data):
    team = standing_data.get("team", {})
    all_stats = standing_data.get("all", {})
    home_stats = standing_data.get("home", {})
    away_stats = standing_data.get("away", {})
    all_goals = all_stats.get("goals", {})
    home_goals = home_stats.get("goals", {})
    away_goals = away_stats.get("goals", {})
    
    return {
        "rank": standing_data.get("rank"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "team_logo": team.get("logo"),
        "points": standing_data.get("points"),
        "goals_diff": standing_data.get("goalsDiff"),
        "group": standing_data.get("group"),
        "form": standing_data.get("form"),
        "status": standing_data.get("status"),
        "description": standing_data.get("description"),
        "all_played": all_stats.get("played"),
        "all_win": all_stats.get("win"),
        "all_draw": all_stats.get("draw"),
        "all_lose": all_stats.get("lose"),
        "all_goals_for": all_goals.get("for"),
        "all_goals_against": all_goals.get("against"),
        "home_played": home_stats.get("played"),
        "home_win": home_stats.get("win"),
        "home_draw": home_stats.get("draw"),
        "home_lose": home_stats.get("lose"),
        "home_goals_for": home_goals.get("for"),
        "home_goals_against": home_goals.get("against"),
        "away_played": away_stats.get("played"),
        "away_win": away_stats.get("win"),
        "away_draw": away_stats.get("draw"),
        "away_lose": away_stats.get("lose"),
        "away_goals_for": away_goals.get("for"),
        "away_goals_against": away_goals.get("against"),
        "update": standing_data.get("update")
    }
    
@dlt.resource(name="standings", write_disposition="replace")
def standings_resource():
    client = APIFootballClient()
    standings_data = client.get_standings(LEAGUE_ID, SEASON)
    
    for standing in standings_data:
        yield flatten_standing(standing)
        
def run_standings_pipeline():
    logger.info("Starting standings pipeline")
    init_audit_table()
    
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="standings_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    try:
        logger.info("Fetching standings data from API")
        standings_gen = standings_resource()
        standings_list = list(standings_gen)
        rows_count = len(standings_list)
        logger.info(f"Fetched {rows_count} standings records")
        
        logger.info("Loading standings data to DuckDB")
        info = pipeline.run(standings_resource())
        
        logger.info(f"Successfully loaded {rows_count} standings to raw.standings")
        
        log_ingestion(
            source_endpoint="standings",
            target_table="standings",
            rows_loaded=rows_count,
            status="success"
        )
        
        return info
    except Exception as e:
        logger.error(f"Standings pipeline failed: {str(e)}")
        log_ingestion(
            source_endpoint="standings",
            target_table="standings",
            rows_loaded=0,
            status=f"failed: {str(e)}"
        )
        raise

if __name__ == "__main__":
    run_standings_pipeline()