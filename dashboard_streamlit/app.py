
import streamlit as st
import duckdb
import os
import sys
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from ingestion.config import DUCKDB_PATH

st.set_page_config(
    page_title="Premier League Data Platform",
    page_icon="⚽",
    layout="wide"
)

def get_db_connection():
    db_path = os.path.abspath(DUCKDB_PATH)
    return duckdb.connect(db_path)

@st.cache_data
def get_team_kpis():
    conn = get_db_connection()
    try:
        query = """
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
        """
        df = conn.execute(query).df()
        return df
    finally:
        conn.close()