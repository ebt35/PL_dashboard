import dlt
import os
import sys
import time

project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.sources.api_football import APIFootballClient
from ingestion.utils.audit import init_audit_table, log_ingestion
from ingestion.utils.logger import setup_logger
from ingestion.config import LEAGUE_ID, SEASON, DUCKDB_PATH
from ingestion.utils.duckdb_setup import get_connection

logger = setup_logger("players_pipeline")


def flatten_player(player_data: dict) -> dict:
    team = player_data.get("team", {})
    games = player_data.get("games", {})
    goals = player_data.get("goals", {})

    return {
        "player_id": player_data.get("id"),
        "player_name": player_data.get("name"),
        "player_firstname": player_data.get("firstname"),
        "player_lastname": player_data.get("lastname"),
        "player_age": player_data.get("age"),
        "player_nationality": player_data.get("nationality"),
        "player_photo": player_data.get("photo"),
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "games_appearances": games.get("appearences"),
        "games_minutes": games.get("minutes"),
        "games_position": games.get("position"),
        "goals": goals.get("total"),
        "assists": goals.get("assists"),
        "goal_involvement": (
            (goals.get("total") or 0) + (goals.get("assists") or 0)
        ),
    }


def get_team_ids():
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT DISTINCT team_id FROM raw.teams"
        ).fetchall()
        return [row[0] for row in rows]
    finally:
        conn.close()


@dlt.resource(name="players", write_disposition="replace")
def players_resource():
    client = APIFootballClient()
    team_ids = get_team_ids()

    for team_id in team_ids:
        logger.info(f"Fetching players for team_id={team_id}")

        players = client.get_players(
            team_id=team_id,
            season=SEASON,
            league=LEAGUE_ID,
        )

        for player in players:
            yield flatten_player(player)


def run_players_pipeline():
    logger.info("Starting players pipeline")
    init_audit_table()

    db_path = os.path.abspath(DUCKDB_PATH)
    pipeline = dlt.pipeline(
        pipeline_name="players_pipeline",
        destination=dlt.destinations.duckdb(credentials=db_path),
        dataset_name="raw",
    )

    try:
        players_gen = players_resource()
        players_list = list(players_gen)
        rows_count = len(players_list)

        logger.info(f"Loading {rows_count} players into DuckDB")
        pipeline.run(players_resource())

        log_ingestion(
            source_endpoint="players",
            target_table="players",
            rows_loaded=rows_count,
            status="success",
        )

        logger.info(f"Successfully loaded {rows_count} players to raw.players")
    except Exception as e:
        logger.error(f"Players pipeline failed: {str(e)}")
        log_ingestion(
            source_endpoint="players",
            target_table="players",
            rows_loaded=0,
            status=f"failed: {str(e)}",
        )
        raise


if __name__ == "__main__":
    run_players_pipeline()
