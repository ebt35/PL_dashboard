import dlt
import os
import sys

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.sources.api_football import APIFootballClient
from ingestion.config import LEAGUE_ID, SEASON, DUCKDB_PATH

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
    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="teams_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw"
    )
    
    info = pipeline.run(teams_resource())
    return info

if __name__ == "__main__":
    run_teams_pipeline()

