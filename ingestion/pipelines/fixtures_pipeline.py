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
