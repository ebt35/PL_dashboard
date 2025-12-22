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