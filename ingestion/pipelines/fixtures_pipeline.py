import dlt
from datetime import datetime, date
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.sources.api_football import APIFootballClient
from ingestion.utils.audit import init_audit_table, log_ingestion, is_first_load
from ingestion.utils.logger import setup_logger
from ingestion.config import LEAGUE_ID, SEASON, DUCKDB_PATH

logger = setup_logger("fixtures_pipeline")

def flatten_fixture(fixture_data):
    fixture = fixture_data.get("fixture", {})
    league = fixture_data.get("league", {})
    teams = fixture_data.get("teams", {})
    goals = fixture_data.get("goals", {})
    score = fixture_data.get("score", {})
    
    return {
        "fixture_id": fixture.get("id"),
        "fixture_date": fixture.get("date"),
        "fixture_timestamp": fixture.get("timestamp"),
        "fixture_timezone": fixture.get("timezone"),
        "fixture_referee": fixture.get("referee"),
        "venue_id": fixture.get("venue", {}).get("id"),
        "venue_name": fixture.get("venue", {}).get("name"),
        "venue_city": fixture.get("venue", {}).get("city"),
        "status_long": fixture_data.get("status", {}).get("long"),
        "status_short": fixture_data.get("status", {}).get("short"),
        "status_elapsed": fixture_data.get("status", {}).get("elapsed"),
        "league_id": league.get("id"),
        "league_name": league.get("name"),
        "league_country": league.get("country"),
        "league_season": league.get("season"),
        "league_round": league.get("round"),
        "home_team_id": teams.get("home", {}).get("id"),
        "home_team_name": teams.get("home", {}).get("name"),
        "home_team_winner": teams.get("home", {}).get("winner"),
        "away_team_id": teams.get("away", {}).get("id"),
        "away_team_name": teams.get("away", {}).get("name"),
        "away_team_winner": teams.get("away", {}).get("winner"),
        "home_goals": goals.get("home"),
        "away_goals": goals.get("away"),
        "score_halftime_home": score.get("halftime", {}).get("home"),
        "score_halftime_away": score.get("halftime", {}).get("away"),
        "score_fulltime_home": score.get("fulltime", {}).get("home"),
        "score_fulltime_away": score.get("fulltime", {}).get("away")
    }

def _get_fixtures_data():
    client = APIFootballClient()
    first_load = is_first_load("fixtures")
    
    if first_load:
        date_from = "2025-01-01"
        date_to = date.today().strftime("%Y-%m-%d")
        return client.get_fixtures(LEAGUE_ID, SEASON, date_from=date_from, date_to=date_to)
    else:
        current_date = date.today().strftime("%Y-%m-%d")
        return client.get_fixtures(LEAGUE_ID, SEASON, date=current_date)
    
    
@dlt.resource(name="fixtures", write_disposition="append")
def fixtures_resource():
    fixtures_data = _get_fixtures_data()
    for fixture in fixtures_data:
        yield flatten_fixture(fixture)

def run_fixtures_pipeline():
    logger.info("Starting fixtures pipeline")
    init_audit_table()
    
    first_load = is_first_load("fixtures")
    if first_load:
        logger.info("First load detected: fetching fixtures from 2025-01-01 to today")
    else:
        logger.info("Incremental load: fetching fixtures for today only")
    
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="fixtures_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    try:
        logger.info("Fetching fixtures data from API")
        fixtures_data = _get_fixtures_data()
        rows_count = len(fixtures_data)
        logger.info(f"Fetched {rows_count} fixtures")
        
        logger.info("Loading fixtures data to DuckDB")
        info = pipeline.run(fixtures_resource())
        
        logger.info(f"Successfully loaded {rows_count} fixtures to raw.fixtures")
        
        log_ingestion(
            source_endpoint="fixtures",
            target_table="fixtures",
            rows_loaded=rows_count,
            status="success"
        )
        
        return info
    except Exception as e:
        logger.error(f"Fixtures pipeline failed: {str(e)}")
        log_ingestion(
            source_endpoint="fixtures",
            target_table="fixtures",
            rows_loaded=0,
            status=f"failed: {str(e)}"
        )
        raise

if __name__ == "__main__":
    run_fixtures_pipeline()