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

logger = setup_logger("scorers_pipeline")

def flatten_scorer(scorer_data):
    team = scorer_data.get("team", {})
    league = scorer_data.get("league", {})
    games = scorer_data.get("games", {})
    goals = scorer_data.get("goals", {})
    
    return {
        "player_id": scorer_data.get("id"),
        "player_name": scorer_data.get("name"),
        "player_firstname": scorer_data.get("firstname"),
        "player_lastname": scorer_data.get("lastname"),
        "player_age": scorer_data.get("age"),
        "player_birth_date": scorer_data.get("birth", {}).get("date") if isinstance(scorer_data.get("birth"), dict) else None,
        "player_birth_place": scorer_data.get("birth", {}).get("place") if isinstance(scorer_data.get("birth"), dict) else None,
        "player_birth_country": scorer_data.get("birth", {}).get("country") if isinstance(scorer_data.get("birth"), dict) else None,
        "player_nationality": scorer_data.get("nationality"),
        "player_height": scorer_data.get("height"),
        "player_weight": scorer_data.get("weight"),
        "player_injured": scorer_data.get("injured"),
        "player_photo": scorer_data.get("photo"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "team_logo": team.get("logo"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "league_country": league.get("country"),
        "league_season": league.get("season"),
        "games_appearances": games.get("appearences"),
        "games_lineups": games.get("lineups"),
        "games_minutes": games.get("minutes"),
        "games_number": games.get("number"),
        "games_position": games.get("position"),
        "games_rating": games.get("rating"),
        "games_captain": games.get("captain"),
        "goals_total": goals.get("total"),
        "goals_assists": goals.get("assists"),
        "goals_conceded": goals.get("conceded")
    }
    
@dlt.resource(name="scorers", write_disposition="replace")
def scorers_resource():
    client = APIFootballClient()
    scorers_data = client.get_top_scorers(LEAGUE_ID, SEASON)
    
    for scorer in scorers_data:
        yield flatten_scorer(scorer)

def run_scorers_pipeline():
    logger.info("Starting scorers pipeline")
    init_audit_table()
    
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="scorers_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    try:
        logger.info("Fetching top scorers data from API")
        scorers_gen = scorers_resource()
        scorers_list = list(scorers_gen)
        rows_count = len(scorers_list)
        logger.info(f"Fetched {rows_count} scorers")
        
        logger.info("Loading scorers data to DuckDB")
        info = pipeline.run(scorers_resource())
        
        logger.info(f"Successfully loaded {rows_count} scorers to raw.scorers")
        
        log_ingestion(
            source_endpoint="players/topscorers",
            target_table="scorers",
            rows_loaded=rows_count,
            status="success"
        )
        
        return info
    except Exception as e:
        logger.error(f"Scorers pipeline failed: {str(e)}")
        log_ingestion(
            source_endpoint="players/topscorers",
            target_table="scorers",
            rows_loaded=0,
            status=f"failed: {str(e)}"
        )
        raise

if __name__ == "__main__":
    run_scorers_pipeline()