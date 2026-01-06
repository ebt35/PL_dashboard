import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from data.queries import get_team_kpis, get_player_kpis, get_last_results, get_next_fixtures


def league_overview():
    st.title("League Overview")
    st.caption("Comprehensive analysis of Premier League team and player performance")
    
    team_df = get_team_kpis()
    player_df = get_player_kpis()
    
    st.header("League Summary Statistics")
    st.caption("Key performance metrics across the Premier League")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Teams", len(team_df))
    with col2:
        st.metric("Goals Scored", int(team_df['goals_scored'].sum()))
    with col3:
        st.metric("Matches Played", int(team_df['matches_played'].sum() / 2))
    
    st.divider()
    
    st.header("Standings")
    st.caption("Current league table showing teams ranked by points and goal difference")
    
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
    st.caption("If teams finish tied in points at the end of the season, score differential is the tie-breaker.")
    
    st.divider()

    st.header("Results & Fixtures")
    st.caption("Latest results and upcoming fixtures for the league")

    tab1, tab2 = st.tabs(["Results", "Fixtures"])

    with tab1:
        results_df = get_last_results(100)
        if results_df.empty:
            st.info("No played matches found.")
        else:
            # clear dateformat
            results_df["date"] = pd.to_datetime(results_df["date"]).dt.strftime("%Y-%m-%d %H:%M")
            results_df.columns = ["Date", "Match", "Score", "Round", "Venue"]
            st.dataframe(results_df, use_container_width=True, hide_index=True)

    with tab2:
        fixtures_df = get_next_fixtures(100)
        if fixtures_df.empty:
            st.info("No upcoming fixtures found.")
        else:
            fixtures_df["date"] = pd.to_datetime(fixtures_df["date"]).dt.strftime("%Y-%m-%d %H:%M")
            fixtures_df.columns = ["Date", "Match", "Round", "Venue"]
            st.dataframe(fixtures_df, use_container_width=True, hide_index=True)


    st.divider()
    
    st.header("Team Performance Analysis")
    st.caption("Visual comparison of team performance metrics across the league")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("Points Distribution")
        st.caption("Shows the points gap between teams. Higher bars indicate stronger league performance. Color intensity represents point totals.")
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
        st.caption("Compares goals scored (green) vs goals conceded (red) for each team. Teams with more green than red bars show a stronger attacking–defensive balance..")
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
    st.caption("Players ranked by goals")
    
    top_scorers = player_df[['player_name', 'team_name', 'goals']].sort_values('goals', ascending=False).copy()
    top_scorers.columns = ['Player', 'Team', 'Goals']
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        scorers_html = "<div style='max-height: 420px; overflow-y: auto;'>"
        scorers_html += "<table style='width: 100%; border-collapse: collapse;'>"
        scorers_html += "<thead><tr>"
        scorers_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
        scorers_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Team</th>"
        scorers_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Goals</th>"
        scorers_html += "</tr></thead><tbody>"
        
        for _, row in top_scorers.head(10).iterrows():
            scorers_html += "<tr style='border-bottom: 1px solid #eee;'>"
            scorers_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
            scorers_html += f"<td style='padding: 10px; text-align: left;'>{row['Team']}</td>"
            scorers_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Goals']) if pd.notna(row['Goals']) else 0}</td>"
            scorers_html += "</tr>"
        
        scorers_html += "</tbody></table></div>"
        st.markdown(scorers_html, unsafe_allow_html=True)
    
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
            xaxis_tickangle=-90,
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
        
    st.divider()
    
    st.header("👟 Top Assist Providers")
    st.caption("Leading players ranked by assists")
    
    top_assist_providers = player_df[['player_name', 'team_name', 'assists']].sort_values('assists', ascending=False).copy()
    top_assist_providers.columns = ['Player', 'Team', 'Assists']
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        assists_html = "<div style='max-height: 420px; overflow-y: auto;'>"
        assists_html += "<table style='width: 100%; border-collapse: collapse;'>"
        assists_html += "<thead><tr>"
        assists_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
        assists_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Team</th>"
        assists_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Assists</th>"
        assists_html += "</tr></thead><tbody>"
        
        for _, row in top_assist_providers.head(10).iterrows():
            assists_html += "<tr style='border-bottom: 1px solid #eee;'>"
            assists_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
            assists_html += f"<td style='padding: 10px; text-align: left;'>{row['Team']}</td>"
            assists_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Assists']) if pd.notna(row['Assists']) else 0}</td>"
            assists_html += "</tr>"
        
        assists_html += "</tbody></table></div>"
        st.markdown(assists_html, unsafe_allow_html=True)
    
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
            xaxis_tickangle=-90,
            height=300,
            showlegend=False
        )
        st.plotly_chart(fig, use_container_width=True)
    
    st.divider()
    
    st.header("Goal Involvement Analysis")
    st.caption("Bubble chart showing the relationship between goals and assists. "
        "Larger bubbles indicate higher total goal involvement.")
    
    st.markdown("*The top-right quadrant highlights players combining goal scoring and creativity.*")

    top_involvement = player_df[['player_name', 'team_name', 'goal_involvement', 'goals', 'assists']].head(20)

    fig = px.scatter(
        top_involvement,
        x='goals',
        y='assists',
        size='goal_involvement',
        color='goal_involvement',
        hover_name='player_name',   
        hover_data={
            'goals': True,
            'assists': True,
            'goal_involvement': True,
            'team_name': True,
            'player_name': False     
        },
        labels={
            'goals': 'Goals',
            'assists': 'Assists',
            'goal_involvement': 'Goal Involvement',
            'team_name': 'Team'
        },
        color_continuous_scale='viridis'
    )

    fig.update_layout(height=500)
    st.plotly_chart(fig, use_container_width=True)

    
    st.divider()

    st.header("Disciplinary Recordss")
    st.caption("The most booked players across the league")

    top_yellow_cards = player_df[['player_name', 'team_name', 'yellow_cards']].sort_values('yellow_cards', ascending=False).copy()
    top_yellow_cards.columns = ['Player', 'Team', 'Yellow Cards']

    top_red_cards = player_df[['player_name', 'team_name', 'red_cards']].sort_values('red_cards', ascending=False).copy()
    top_red_cards.columns = ['Player', 'Team', 'Red Cards']

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("Yellow cards 🟨")
        yellow_html = "<div style='max-height: 420px; overflow-y: auto;'>"
        yellow_html += "<table style='width: 100%; border-collapse: collapse;'>"
        yellow_html += "<thead><tr>"
        yellow_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
        yellow_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Team</th>"
        yellow_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Yellow Cards</th>"
        yellow_html += "</tr></thead><tbody>"

        for _, row in top_yellow_cards.head(10).iterrows():
            yellow_html += "<tr style='border-bottom: 1px solid #eee;'>"
            yellow_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
            yellow_html += f"<td style='padding: 10px; text-align: left;'>{row['Team']}</td>"
            yellow_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Yellow Cards']) if pd.notna(row['Yellow Cards']) else 0}</td>"
            yellow_html += "</tr>"

        yellow_html += "</tbody></table></div>"
        st.markdown(yellow_html, unsafe_allow_html=True)

    with col2:
        st.subheader("Red cards 🟥")
        red_html = "<div style='max-height: 420px; overflow-y: auto;'>"
        red_html += "<table style='width: 100%; border-collapse: collapse;'>"
        red_html += "<thead><tr>"
        red_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
        red_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Team</th>"
        red_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Red Cards</th>"
        red_html += "</tr></thead><tbody>"

        for _, row in top_red_cards.head(10).iterrows():
            red_html += "<tr style='border-bottom: 1px solid #eee;'>"
            red_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
            red_html += f"<td style='padding: 10px; text-align: left;'>{row['Team']}</td>"
            red_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Red Cards']) if pd.notna(row['Red Cards']) else 0}</td>"
            red_html += "</tr>"

        red_html += "</tbody></table></div>"
        st.markdown(red_html, unsafe_allow_html=True)
