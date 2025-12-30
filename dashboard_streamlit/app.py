
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

    st.divider()
    
    # Architecture PDF
    st.header("System Architecture")
    pdf_path = os.path.join(project_root, "docs", "Football_pipeline_architecture.pdf")
    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as pdf_file:
            st.download_button(
                label="Download Architecture Document",
                data=pdf_file,
                file_name="Football_pipeline_architecture.pdf",
                mime="application/pdf"
            )
        st.caption("Click above to download the complete system architecture documentation")
    else:
        st.info("Architecture document not found. Please ensure the PDF is located in the docs folder.") 
        
# =========================
# LEAGUE OVERVIEW
# =========================

def league_overview():
    st.title("League Overview")
    st.caption("Comprehensive analysis of Premier League team and player performance")
    
    team_df = get_team_kpis()
    player_df = get_player_kpis()
    
    st.header("League Summary Statistics")
    st.caption("Key metrics across all teams in the Premier League")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Teams", len(team_df))
    with col2:
        st.metric("Total Goals Scored", int(team_df['goals_scored'].sum()))
    with col3:
        st.metric("Total Goals Conceded", int(team_df['goals_conceded'].sum()))
    with col4:
        st.metric("Total Matches Played", int(team_df['matches_played'].sum() / 2))
    
    st.divider()
    
    st.header("Standings")
    st.caption("Current league table showing team positions ranked by points and goal difference")
    
  # Create standings with logos
    standings_html = "<div style='max-height: 1100px; overflow-y: auto;'>"
    standings_html += "<table style='width: 75%; border-collapse: collapse;'>"
    standings_html += "<thead><tr style='background-color: #f0f0f0; position: sticky; top: 0;'>"
    standings_html += "<th style='padding: 10px; text-align: center;'>Pos</th>"
    standings_html += "<th style='padding: 10px; text-align: left;'>Team</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>MP</th>"
    standings_html += "<th style='padding:10px; text-align:center;'>W</th>"
    standings_html += "<th style='padding:10px; text-align:center;'>D</th>"
    standings_html += "<th style='padding:10px; text-align:center;'>L</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>GF</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>GA</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>GD</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>Pts</th>"
    standings_html += "<th style='padding: 10px; text-align: center;'>Form</th>"
    standings_html += "</tr></thead><tbody>"
    
    for idx, row in team_df.iterrows():
        pos = idx + 1
        logo_html = f"<img src='{row['team_logo']}' style='width: 30px; height: 30px; margin-right: 10px; vertical-align: middle;' onerror='this.style.display=\"none\"'>" if row.get('team_logo') else ""
        standings_html += f"<tr style='border-bottom: 1px solid #ddd;'>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{pos}</td>"
        standings_html += f"<td style='padding: 10px;'>{logo_html}<strong>{row['team_name']}</strong></td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{int(row['matches_played'])}</td>"
        standings_html += f"<td style='padding:10px; text-align:center;'>{int(row['wins'])}</td>"
        standings_html += f"<td style='padding:10px; text-align:center;'>{int(row['draws'])}</td>"
        standings_html += f"<td style='padding:10px; text-align:center;'>{int(row['losses'])}</td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{int(row['goals_scored'])}</td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{int(row['goals_conceded'])}</td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{int(row['goal_difference'])}</td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'><strong>{int(row['total_points'])}</strong></td>"
        standings_html += f"<td style='padding: 10px; text-align: center;'>{row.get('form', '')}</td>"
        standings_html += "</tr>"
    
    standings_html += "</tbody></table></div>"
    st.markdown(standings_html, unsafe_allow_html=True)
    
    st.divider()
    
    st.header("Team Performance Analysis")
    st.caption("Visual comparison of team performance metrics across the league")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Points Distribution")
        st.caption("Shows the points gap between teams. Higher bars indicate stronger teams. Color intensity represents point totals.")
        fig = px.bar(
            team_df,
            x='team_name',
            y='total_points',
            labels={'team_name': 'Team', 'total_points': 'Points'},
            color='total_points',
            color_continuous_scale='viridis'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            height=400,
            showlegend=False,
            yaxis_title="Points"
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("Attacking vs Defensive Performance")
        st.caption("Compares goals scored (green) vs goals conceded (red) for each team. Teams with more green than red have better attacking/defensive balance.")
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=team_df['team_name'],
            y=team_df['goals_scored'],
            name='Goals Scored',
            marker_color='#2ecc71'
        ))
        fig.add_trace(go.Bar(
            x=team_df['team_name'],
            y=team_df['goals_conceded'],
            name='Goals Conceded',
            marker_color='#e74c3c'
        ))
        fig.update_layout(
            xaxis_tickangle=-45,
            height=400,
            barmode='group',
            yaxis_title="Goals"
        )
        st.plotly_chart(fig, use_container_width=True)
        
    st.divider()
    
    st.header("⚽ Top Goal Scorers")
    st.caption("Leading players ranked by total goals")
    
    top_scorers = player_df[['player_name', 'team_name', 'goals']].sort_values('goals', ascending=False).copy()
    top_scorers.columns = ['Player', 'Team', 'Goals']
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.dataframe(top_scorers.head(10), use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("Top 5 Goal Scorers")
        st.caption("Visual representation of the league's most prolific scorers")
        fig = px.bar(
            top_scorers.head(5),
            x='Player',
            y='Goals',
            labels={'Player': 'Player', 'Goals': 'Goals'},
            color='Goals',
            color_continuous_scale='plasma'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
    st.divider()
    
    st.header("👟 Top Assist Providers")
    st.caption("Leading players ranked by total assists")
    
    top_assist_providers= player_df[['player_name', 'team_name', 'assists']].sort_values('assists', ascending=False).copy()
    top_assist_providers.columns = ['Player', 'Team', 'Assists']
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.dataframe(top_assist_providers.head(10), use_container_width=True, hide_index=True)
    
    with col2:
        st.subheader("Top 5 Assist Providers")
        st.caption("Visual representation of the league's most assist providers")
        fig = px.bar(
            top_assist_providers.head(5),
            x='Player',
            y='Assists',
            labels={'Player': 'Player', 'Assists': 'Assists'},
            color='Assists',
            color_continuous_scale='plasma'
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
    st.divider()
    
    st.header("Goal Involvement Analysis")
    st.caption("Bubble chart showing the relationship between goals and assists. Larger bubbles indicate higher total goal involvement. Players in the top-right are the most impactful.")
    
    top_involvement = player_df[['player_name', 'team_name', 'goal_involvement', 'goals', 'assists']].head(20)
    
    fig = px.scatter(
        top_involvement,
        x='goals',
        y='assists',
        size='goal_involvement',
        hover_data=['player_name', 'team_name'],
        labels={'goals': 'Goals', 'assists': 'Assists', 'goal_involvement': 'Goal Involvement'},
        color='goal_involvement',
        color_continuous_scale='viridis'
    )
    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)
    
# =========================
# TEAM OVERVIEW
# =========================

def team_overview():
    st.title("Team Overview")
    st.caption("Detailed analysis of individual team performance and player statistics")
    
    team_df = get_team_kpis()
    
    selected_team = st.selectbox(
        "Select Team",
        options=sorted(team_df['team_name'].unique()),
        index=0
    )
    
    team_data = team_df[team_df['team_name'] == selected_team].iloc[0]
    player_df = get_team_player_kpis(selected_team)
    
    team_rank = team_df[team_df['team_name'] == selected_team].index[0] + 1
    team_logo = team_data.get('team_logo', None)
    
    # Display team logo and name
    col1, col2 = st.columns([1, 4])
    with col1:
        if team_logo:
            st.image(team_logo, width=100)
        else:
            st.write("")  # Spacer if no logo
    with col2:
        st.header(f"{selected_team}")
        st.caption(f"Current League Position: {team_rank}")
        
        st.markdown(
    f"""
    **Founded:** {int(team_data['team_founded']) if pd.notna(team_data['team_founded']) else 'N/A'}  
    **Venue:** {team_data['venue_name']}  
    **City:** {team_data['venue_city']}  
    **Capacity:** {int(team_data['venue_capacity']) if pd.notna(team_data['venue_capacity']) else 'N/A'}
    """
)
        
    st.divider()
    
    st.subheader("Core Team Performance Metrics")
    st.caption("Essential KPIs showing team's competitive performance in the league")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Matches Played",
            int(team_data['matches_played']),
            help="Total matches played in the season"
        )
    with col2:
        st.metric(
            "Total Points",
            int(team_data['total_points']),
            help="Total points accumulated (3 for win, 1 for draw)"
        )
    with col3:
        st.metric(
            "Goals Scored",
            int(team_data['goals_scored']),
            help="Total goals scored by the team"
        )
    with col4:
        st.metric(
            "Goals Conceded",
            int(team_data['goals_conceded']),
            help="Total goals conceded by the team"
        )
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric(
            "Goal Difference",
            int(team_data['goal_difference']),
            delta=f"{int(team_data['goal_difference']) - team_df['goal_difference'].mean():.1f} vs avg",
            help="Difference between goals scored and conceded"
        )
    with col2:
        st.metric(
            "Win Rate",
            f"{team_data['win_rate']:.1f}%",
            delta=f"{team_data['win_rate'] - team_df['win_rate'].mean():.1f}% vs avg",
            help="Percentage of matches won"
        )
    with col3:
        st.metric(
            "Points per Match",
            f"{team_data['points_per_match']:.2f}",
            delta=f"{team_data['points_per_match'] - team_df['points_per_match'].mean():.2f} vs avg",
            help="Average points earned per match"
        )
        
    st.divider()
    
    st.subheader("Performance Benchmarking")
    st.caption("Compare team performance against league averages to identify strengths and weaknesses")
    
    league_avg = {
        'points': team_df['total_points'].mean(),
        'goals_scored': team_df['goals_scored'].mean(),
        'goals_conceded': team_df['goals_conceded'].mean(),
        'goal_difference': team_df['goal_difference'].mean()
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Team vs League Average Comparison**")
        st.caption("Blue bars show team performance, gray bars show league average. Higher blue bars indicate above-average performance.")
        comparison_data = pd.DataFrame({
            'Metric': ['Points', 'Goals Scored', 'Goals Conceded', 'Goal Difference'],
            'Team': [
                team_data['total_points'],
                team_data['goals_scored'],
                team_data['goals_conceded'],
                team_data['goal_difference']
            ],
            'League Avg': [
                league_avg['points'],
                league_avg['goals_scored'],
                league_avg['goals_conceded'],
                league_avg['goal_difference']
            ]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=comparison_data['Metric'],
            y=comparison_data['Team'],
            name='Team',
            marker_color='#3498db'
        ))
        fig.add_trace(go.Bar(
            x=comparison_data['Metric'],
            y=comparison_data['League Avg'],
            name='League Average',
            marker_color='#95a5a6'
        ))
        fig.update_layout(
            barmode='group',
            height=400,
            yaxis_title="Value",
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("**League Standings Context**")
        st.caption("Red bar highlights selected team's position. Shows how team ranks among all league teams by points.")
        position_data = team_df[['team_name', 'total_points']].copy()
        position_data['is_selected'] = position_data['team_name'] == selected_team
        
        fig = px.bar(
            position_data,
            x='team_name',
            y='total_points',
            color='is_selected',
            color_discrete_map={True: '#e74c3c', False: '#3498db'},
            labels={'team_name': 'Team', 'total_points': 'Points'},
            height=400
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            showlegend=False,
            yaxis_title="Points"
        )
        st.plotly_chart(fig, use_container_width=True)
        
    st.divider()
    
    st.subheader("Performance Benchmarking")
    st.caption("Compare team performance against league averages to identify strengths and weaknesses")
    
    league_avg = {
        'points': team_df['total_points'].mean(),
        'goals_scored': team_df['goals_scored'].mean(),
        'goals_conceded': team_df['goals_conceded'].mean(),
        'goal_difference': team_df['goal_difference'].mean()
    }
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Team vs League Average Comparison**")
        st.caption("Blue bars show team performance, gray bars show league average. Higher blue bars indicate above-average performance.")
        comparison_data = pd.DataFrame({
            'Metric': ['Points', 'Goals Scored', 'Goals Conceded', 'Goal Difference'],
            'Team': [
                team_data['total_points'],
                team_data['goals_scored'],
                team_data['goals_conceded'],
                team_data['goal_difference']
            ],
            'League Avg': [
                league_avg['points'],
                league_avg['goals_scored'],
                league_avg['goals_conceded'],
                league_avg['goal_difference']
            ]
        })
        
        fig = go.Figure()
        fig.add_trace(go.Bar(
            x=comparison_data['Metric'],
            y=comparison_data['Team'],
            name='Team',
            marker_color='#3498db'
        ))
        fig.add_trace(go.Bar(
            x=comparison_data['Metric'],
            y=comparison_data['League Avg'],
            name='League Average',
            marker_color='#95a5a6'
        ))
        fig.update_layout(
            barmode='group',
            height=400,
            yaxis_title="Value",
            showlegend=True
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("**League Standings Context**")
        st.caption("Red bar highlights selected team's position. Shows how team ranks among all league teams by points.")
        position_data = team_df[['team_name', 'total_points']].copy()
        position_data['is_selected'] = position_data['team_name'] == selected_team
        
        fig = px.bar(
            position_data,
            x='team_name',
            y='total_points',
            color='is_selected',
            color_discrete_map={True: '#e74c3c', False: '#3498db'},
            labels={'team_name': 'Team', 'total_points': 'Points'},
            height=400
        )
        fig.update_layout(
            xaxis_tickangle=-45,
            showlegend=False,
            yaxis_title="Points"
        )
        st.plotly_chart(fig, use_container_width=True)