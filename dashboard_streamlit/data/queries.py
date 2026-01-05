# import streamlit as st
# from .db import get_db_connection

# @st.cache_data
# def get_team_kpis():
#     conn = get_db_connection()
#     try:
#         return conn.execute("""
#             SELECT DISTINCT
#                 team_name,
#                 team_logo,
#                 matches_played,
#                 total_points,
#                 wins,
#                 draws,
#                 losses,
#                 goals_scored,
#                 goals_conceded,
#                 goal_difference,
#                 win_rate,
#                 points_per_match,
#                 form,
#                 team_founded,
#                 venue_name,
#                 venue_city,
#                 venue_capacity
#             FROM main_mart.mart_team_kpi
#             ORDER BY total_points DESC, goal_difference DESC
#         """).df()
#     finally:
#         conn.close()

# @st.cache_data
# def get_player_kpis():
#     conn = get_db_connection()
#     try:
#         query = """
#             SELECT DISTINCT
#                 player_name,
#                 team_name,
#                 goals,
#                 assists,
#                 yellow_cards,
#                 red_cards,
#                 goal_involvement,
#                 games_appearances,
#                 goals_per_game,
#                 assists_per_game,
#                 goal_involvement_per_game
#             FROM main_mart.mart_player_kpi
#             ORDER BY goal_involvement DESC, goals DESC
#         """
#         df = conn.execute(query).df()
#         return df
#     finally:
#         conn.close()


# @st.cache_data
# def get_team_player_kpis(team_name):
#     conn = get_db_connection()
#     try:
#         query = """
#             SELECT DISTINCT
#                 player_name,
#                 goals,
#                 assists,
#                 yellow_cards,
#                 red_cards,
#                 goal_involvement,
#                 games_appearances,
#                 goals_per_game,
#                 assists_per_game
#             FROM main_mart.mart_player_kpi
#             WHERE team_name = ?
#             ORDER BY goal_involvement DESC, goals DESC
#         """
#         df = conn.execute(query, [team_name]).df()
#         return df
#     finally:
#         conn.close()

# @st.cache_data
# def get_source_data(table_name):
#     conn = get_db_connection()
#     try:
#         query = f"SELECT DISTINCT * FROM main_src.{table_name} LIMIT 1000"
#         df = conn.execute(query).df()
#         return df
#     finally:
#         conn.close()
    
import streamlit as st
import pandas as pd  
from .db import get_db_connection

@st.cache_data
def get_team_kpis():
    conn = get_db_connection()
    try:
        return conn.execute("""
            SELECT DISTINCT
                team_name,
                team_logo,
                matches_played,
                total_points,
                wins,
                draws,
                losses,
                goals_scored,
                goals_conceded,
                goal_difference,
                win_rate,
                points_per_match,
                form,
                team_founded,
                venue_name,
                venue_city,
                venue_capacity
            FROM main_mart.mart_team_kpi
            ORDER BY total_points DESC, goal_difference DESC
        """).df()
    finally:
        conn.close()


@st.cache_data
def get_player_kpis():
    conn = get_db_connection()
    try:
        query = """
            SELECT DISTINCT
                player_name,
                team_name,
                goals,
                assists,
                yellow_cards,
                red_cards,
                goal_involvement,
                games_appearances,
                goals_per_game,
                assists_per_game,
                goal_involvement_per_game
            FROM main_mart.mart_player_kpi
            ORDER BY goal_involvement DESC, goals DESC
        """
        df = conn.execute(query).df()
        return df
    finally:
        conn.close()


@st.cache_data
def get_team_player_kpis(team_name):
    conn = get_db_connection()
    try:
        query = """
            SELECT DISTINCT
                player_name,
                goals,
                assists,
                yellow_cards,
                red_cards,
                goal_involvement,
                games_appearances,
                goals_per_game,
                assists_per_game
            FROM main_mart.mart_player_kpi
            WHERE team_name = ?
            ORDER BY goal_involvement DESC, goals DESC
        """
        df = conn.execute(query, [team_name]).df()
        return df
    finally:
        conn.close()


@st.cache_data
def get_source_data(table_name):
    conn = get_db_connection()
    try:
        query = f"SELECT DISTINCT * FROM main_src.{table_name} LIMIT 1000"
        df = conn.execute(query).df()
        return df
    finally:
        conn.close()


@st.cache_data
def get_last_results(limit: int = 50) -> pd.DataFrame:
    conn = get_db_connection()
    try:
        query = f"""
            WITH dedup AS (
                SELECT
                    fixture_id,
                    fixture_date,
                    fixture_timestamp,
                    league_round,
                    venue_name,
                    home_team_name,
                    away_team_name,
                    score_fulltime_home,
                    score_fulltime_away,
                    _dlt_load_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY fixture_id
                        ORDER BY fixture_timestamp DESC, _dlt_load_id DESC
                    ) AS rn
                FROM raw.fixtures
            )
            SELECT
                fixture_date AS date,
                home_team_name || ' vs ' || away_team_name AS match,
                CAST(score_fulltime_home AS VARCHAR) || ' - ' || CAST(score_fulltime_away AS VARCHAR) AS score,
                league_round AS round,
                venue_name AS venue
            FROM dedup
            WHERE rn = 1
              AND score_fulltime_home IS NOT NULL
              AND score_fulltime_away IS NOT NULL
            ORDER BY fixture_date DESC
            LIMIT {int(limit)};
        """
        return conn.execute(query).df()
    finally:
        conn.close()


@st.cache_data
def get_next_fixtures(limit: int = 50) -> pd.DataFrame:
    conn = get_db_connection()
    try:
        query = f"""
            WITH dedup AS (
                SELECT
                    fixture_id,
                    fixture_date,
                    fixture_timestamp,
                    league_round,
                    venue_name,
                    home_team_name,
                    away_team_name,
                    score_fulltime_home,
                    score_fulltime_away,
                    _dlt_load_id,
                    ROW_NUMBER() OVER (
                        PARTITION BY fixture_id
                        ORDER BY fixture_timestamp DESC, _dlt_load_id DESC
                    ) AS rn
                FROM raw.fixtures
            )
            SELECT
                fixture_date AS date,
                home_team_name || ' vs ' || away_team_name AS match,
                league_round AS round,
                venue_name AS venue
            FROM dedup
            WHERE rn = 1
              AND score_fulltime_home IS NULL
              AND score_fulltime_away IS NULL
              AND fixture_date >= CURRENT_TIMESTAMP
            ORDER BY fixture_date ASC
            LIMIT {int(limit)};
        """
        return conn.execute(query).df()
    finally:
        conn.close()
