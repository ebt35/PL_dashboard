import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from data.queries import get_team_kpis, get_player_kpis

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
    