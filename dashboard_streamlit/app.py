
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
        

# =========================
# HOME
# =========================

def home_page():
    st.title("Premier League Data Platform")
    st.caption("Comprehensive analytics and insights for the 2025/2026 Premier League season")
    
    # Logo
    logo_path = os.path.join(project_root, "utils", "hero-banner.png")
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=True)
    
    st.divider()
    
  # Premier League Description
    st.header("About the Premier League")
    st.markdown("""
    The **Premier League** is the top tier of English football, featuring 20 of the best clubs in England. 
    Established in 1992, it has become one of the most watched and competitive football leagues in the world.
    
    This platform provides comprehensive data analytics for the **2025/2026 season**, including:
    - **Team Performance Metrics**: Points, goals, win rates, and league standings
    - **Player Statistics**: Goals, assists, and goal involvement
    - **Match Data**: Fixtures, results, and detailed match information
    - **Real-time Insights**: Interactive visualizations and KPI dashboards
    
    Navigate through the pages to explore detailed analytics and insights.
    """)
    