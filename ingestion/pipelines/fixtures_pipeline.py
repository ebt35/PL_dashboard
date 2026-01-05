import dlt
from datetime import datetime, date
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.sources.api_football import APIFootballClient
from ingestion.config import LEAGUE_ID, SEASON, DUCKDB_PATH

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

from datetime import date, timedelta

def _get_fixtures_data():
    try:
        client = APIFootballClient()

        #fetch 60 days back and 60 days ahead (adjust as needed)
        date_from = (date.today() - timedelta(days=60)).strftime("%Y-%m-%d")
        date_to   = (date.today() + timedelta(days=60)).strftime("%Y-%m-%d")

        fixtures = client.get_fixtures(LEAGUE_ID, SEASON, date_from=date_from, date_to=date_to)
        return fixtures if fixtures else []
    except Exception as e:
        print(f"Error fetching fixtures data: {str(e)}")
        raise

@dlt.resource(name="fixtures", write_disposition="merge", primary_key="fixture_id")
def fixtures_resource():
    fixtures_data = _get_fixtures_data()
    for fixture in fixtures_data:
        yield flatten_fixture(fixture)

def run_fixtures_pipeline():
    import duckdb
    
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="fixtures_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    # Get fixtures data first to check if we have data
    fixtures_data = _get_fixtures_data()
    
    # If no data, ensure table structure exists
    if not fixtures_data:
        conn = duckdb.connect(db_path)
        try:
            # Check if table exists
            result = conn.execute("""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = 'raw' AND table_name = 'fixtures'
            """).fetchone()
            
            # If table doesn't exist, create empty table with proper schema
            if result[0] == 0:
                conn.execute("""
                    CREATE TABLE IF NOT EXISTS raw.fixtures (
                        fixture_id INTEGER,
                        fixture_date VARCHAR,
                        fixture_timestamp BIGINT,
                        fixture_timezone VARCHAR,
                        fixture_referee VARCHAR,
                        venue_id INTEGER,
                        venue_name VARCHAR,
                        venue_city VARCHAR,
                        status_long VARCHAR,
                        status_short VARCHAR,
                        status_elapsed INTEGER,
                        league_id INTEGER,
                        league_name VARCHAR,
                        league_country VARCHAR,
                        league_season INTEGER,
                        league_round VARCHAR,
                        home_team_id INTEGER,
                        home_team_name VARCHAR,
                        home_team_winner BOOLEAN,
                        away_team_id INTEGER,
                        away_team_name VARCHAR,
                        away_team_winner BOOLEAN,
                        home_goals INTEGER,
                        away_goals INTEGER,
                        score_halftime_home INTEGER,
                        score_halftime_away INTEGER,
                        score_fulltime_home INTEGER,
                        score_fulltime_away INTEGER
                    )
                """)
        finally:
            conn.close()
        return {"status": "success", "rows": 0, "message": "No fixtures for today, table structure created"}
    
    # Run pipeline with data
    info = pipeline.run(fixtures_resource())
    return info

if __name__ == "__main__":
    run_fixtures_pipeline()

