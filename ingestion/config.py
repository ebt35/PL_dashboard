import os
from dotenv import load_dotenv

load_dotenv()

API_BASE_URL = "https://v3.football.api-sports.io"
API_KEY = os.getenv("API_FOOTBALL_KEY")
LEAGUE_ID = 39
SEASON = 2025

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
duckdb_dir = os.path.join(project_root, "duckdb")
os.makedirs(duckdb_dir, exist_ok=True)
DUCKDB_PATH = os.path.join(duckdb_dir, "football.duckdb")
