import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from utils.plotly_theme import apply_dark_plotly
from data.queries import get_team_kpis, get_team_player_kpis

def pluralize(value, singular, plural=None):
    if plural is None:
        plural = singular + "s"
    return f"{int(value)} {singular if value == 1 else plural}"

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
    
    col1, col2 = st.columns([1, 8], gap="small")

    with col1:
        st.markdown("<br>", unsafe_allow_html=True)
        if team_logo:
            st.image(team_logo, width=120)

    with col2:
        st.header(selected_team)
        st.caption(f"League Position: {team_rank}")

        st.subheader("Club Information")
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
    st.caption("Key indicators summarising the team’s performance in the league")
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric(
            "Matches Played",
            int(team_data['matches_played']),
            help="Total number of league matches played"
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
            help="Total goals scored across all league matches"
        )
    with col4:
        st.metric(
            "Goals Conceded",
            int(team_data['goals_conceded']),
            help="Total goals conceded across all league matches"
        )
    
    col1, col2, col3 = st.columns(3)
    with col1:
        gd_delta = team_data["goal_difference"] - team_df["goal_difference"].mean()
        st.metric(
            "Goal Difference",
            int(team_data["goal_difference"]),
            delta=f"{gd_delta:+.1f} vs league avg",
            help="Goals scored minus goals conceded"
        )
    with col2:
        st.metric(
            "Win Rate",
            f"{team_data['win_rate']:.0f}%",
            delta=f"{team_data['win_rate'] - team_df['win_rate'].mean():.1f}% vs league avg",
            help="Percentage of matches won"
        )
    with col3:
        ppm_delta = team_data["points_per_match"] - team_df["points_per_match"].mean()
        st.metric(
            "Points per Match",
            f"{team_data['points_per_match']:.2f}",
            delta=f"{ppm_delta:+.2f} vs league avg",
            help="Average number of points earned per match"
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
            'Metric': ['Goals Scored', 'Goals Conceded', 'Goal Difference', 'Points'],
            'Team': [
                team_data['goals_scored'],
                team_data['goals_conceded'],
                team_data['goal_difference'],
                team_data['total_points'],
            ],
            'League Avg': [
                league_avg['goals_scored'],
                league_avg['goals_conceded'],
                league_avg['goal_difference'],
                league_avg['points'],
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
            marker_color='#22c55e'
        ))
        fig.update_layout(
            barmode='group',
            height=400,
            yaxis_title="Value",
            showlegend=True
        )
        fig = apply_dark_plotly(fig)

        st.plotly_chart(fig, width="stretch")
    
    with col2:
        st.markdown("**League Standings Context**")
        st.caption(
            "Red bar highlights the selected team. Shows how the team ranks among all league teams by points."
        )

        position_data = team_df[
            ["team_name", "total_points", "rank"]
        ].copy()

        # Sort by actual league position
        position_data = position_data.sort_values("rank")

        # Highlight selected team
        position_data["bar_color"] = position_data["team_name"].apply(
            lambda x: "#e74c3c" if x == selected_team else "#3498db"
        )

        fig = px.bar(
            position_data,
            x="team_name",
            y="total_points",
            color="bar_color",
            color_discrete_map="identity",
            labels={"team_name": "Team", "total_points": "Points"},
            height=400,
        )

        fig.update_layout(
            xaxis_tickangle=-90,
            showlegend=False,
            yaxis_title="Points",
        )

        fig.update_xaxes(
            categoryorder="array",
            categoryarray=position_data["team_name"],
        )

        fig = apply_dark_plotly(fig)

        st.plotly_chart(fig, width="stretch")
    
    st.divider()
    
    st.subheader("Squad Performance Metrics")
    st.caption("How individual players contribute to the team's attacking output")

    if not player_df.empty:
        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            display_df = player_df[['player_name', 'goals', 'assists', 'goal_involvement', 'games_appearances']].copy()
            display_df.columns = ['Player', 'Goals', 'Assists', 'Goal Involvement', 'Appearances']

            table_html = "<div style='max-height: 600px; overflow-y: auto;'>"
            table_html += "<table style='width: 100%; border-collapse: collapse;'>"
            table_html += "<thead><tr>"
            table_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
            table_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Goals</th>"
            table_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Assists</th>"
            table_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Goal Involvement</th>"
            table_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Appearances</th>"
            table_html += "</tr></thead><tbody>"

            for _, row in display_df.iterrows():
                table_html += "<tr style='border-bottom: 1px solid #eee;'>"
                table_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
                table_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Goals']) if pd.notna(row['Goals']) else 0}</td>"
                table_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Assists']) if pd.notna(row['Assists']) else 0}</td>"
                table_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Goal Involvement']) if pd.notna(row['Goal Involvement']) else 0}</td>"
                table_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Appearances']) if pd.notna(row['Appearances']) else 0}</td>"
                table_html += "</tr>"

            table_html += "</tbody></table></div>"
            st.markdown(table_html, unsafe_allow_html=True)

        with col2:
            st.markdown("**Top 3 Goal Scorers**")
            st.caption("Players with the most goals")
            top_goals = player_df.sort_values(by="goals", ascending=False).head(3)
            for _, row in top_goals.iterrows():
                st.metric(row["player_name"],pluralize(row["goals"], "Goal"))

        with col3:
            st.markdown("**Top 3 Assist Leaders**")
            st.caption("Players with the most assists")
            top_assists = player_df.sort_values(by="assists", ascending=False).head(3)
            for _, row in top_assists.iterrows():
                st.metric(row["player_name"],pluralize(row["assists"], "Assist"))
                
        st.divider()

        st.subheader("Player Performance Analysis")
        st.caption("Detailed analysis of player contributions to team's attacking output")

        col1, col2 = st.columns(2)

        with col1:
            st.markdown("**Goals and Assists Distribution**")
            st.caption("Shows each player's balance between scoring goals and providing assists. Players strong in both areas tend to be the most valuable contributors.")

            fig = px.bar(
                player_df.head(10),
                x="player_name",
                y=["goals", "assists"],
                barmode="group",
                labels={
                    "player_name": "Player",
                    "value": "Goals / Assists",
                    "variable": "Metric"
                },
                color_discrete_map={
                    "goals": "#1f77b4",    
                    "assists": "#ff7f0e"   
                },
                title="Goals vs Assists by Player"
            )

            fig.update_layout(
                xaxis_tickangle=-45,
                legend_title_text="",
                height=400
            )

            fig = apply_dark_plotly(fig)

            st.plotly_chart(fig, width="stretch")
        
        with col2:
            st.markdown("**Goal Involvement Analysis**")
            st.caption(
                "Bubble size represents total goal involvement. Players in the top-right quadrant "
                "combine scoring and creativity, indicating higher attacking impact."
            )

            fig = px.scatter(
                player_df,
                x="goals",
                y="assists",
                size="goal_involvement",
                color="goal_involvement",
                hover_name="player_name",
                hover_data={
                    "goals": False,
                    "assists": False,
                    "goal_involvement": False
                },
                labels={
                    "goals": "Goals",
                    "assists": "Assists",
                    "goal_involvement": "Goal Involvement"
                },
                color_continuous_scale="plasma"
            )

            fig.update_traces(
                hovertemplate=
                "<b>%{hovertext}</b><br><br>"
                "Goals: %{x}<br>"
                "Assists: %{y}<br>"
                "Goal Involvement: %{marker.size}"
                "<extra></extra>"
            )

            fig.update_layout(
                height=450,
                xaxis=dict(showgrid=True, gridcolor="rgba(200,200,200,0.2)"),
                yaxis=dict(showgrid=True, gridcolor="rgba(200,200,200,0.2)")
            )

            fig = apply_dark_plotly(fig)

            st.plotly_chart(fig, width="stretch")

        st.divider()

        st.subheader("🟨🟥 Disciplinary Records (Squad)")
        st.caption("Summary of yellow and red cards accumulated by squad players.")

        cards_df = player_df[["player_name", "yellow_cards", "red_cards"]].copy()
        cards_df.columns = ["Player", "Yellow Cards", "Red Cards"]
        cards_df = cards_df.sort_values(by=["Yellow Cards", "Red Cards"], ascending=False)

        cards_html = "<div style='max-height: 600px; overflow-y: auto;'>"
        cards_html += "<table style='width: 50%; border-collapse: collapse;'>"
        cards_html += "<thead><tr>"
        cards_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Player</th>"
        cards_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Yellow Cards</th>"
        cards_html += "<th style='padding: 10px; text-align: left; border-bottom: 1px solid #ddd;'>Red Cards</th>"
        cards_html += "</tr></thead><tbody>"

        for _, row in cards_df.iterrows():
            cards_html += "<tr style='border-bottom: 1px solid #eee;'>"
            cards_html += f"<td style='padding: 10px; text-align: left;'>{row['Player']}</td>"
            cards_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Yellow Cards']) if pd.notna(row['Yellow Cards']) else 0}</td>"
            cards_html += f"<td style='padding: 10px; text-align: left;'>{int(row['Red Cards']) if pd.notna(row['Red Cards']) else 0}</td>"
            cards_html += "</tr>"

        cards_html += "</tbody></table></div>"
        st.markdown(cards_html, unsafe_allow_html=True)

    else:
        st.info("No player data available for this team.")

