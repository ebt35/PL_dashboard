import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd

from data.queries import get_team_kpis, get_team_player_kpis

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
    
    st.subheader("Squad Players Statistics")
    st.caption("Individual player contributions to team's attacking performance")
    
    if not player_df.empty:
        col1, col2, col3 = st.columns([3, 1, 1])

        with col1:
            display_df = player_df[['player_name', 'goals', 'assists', 'goal_involvement', 'games_appearances']].copy()
            display_df.columns = ['Player', 'Goals', 'Assists', 'Goal Involvement', 'Appearances']
            st.dataframe(display_df, use_container_width=True, hide_index=True)

        with col2:
            st.markdown("**Top 3 Goal Scorers**")
            st.caption("Players with the most goals")
            top_goals = player_df.sort_values(by="goals", ascending=False).head(3)
            for _, row in top_goals.iterrows():
                st.metric(row['player_name'], f"{int(row['goals'])} Goals")

        with col3:
            st.markdown("**Top 3 Assist Providers**")
            st.caption("Players with the most assists")
            top_assists = player_df.sort_values(by="assists", ascending=False).head(3)
            for _, row in top_assists.iterrows():
                st.metric(row['player_name'], f"{int(row['assists'])} Assists")

                
        st.subheader("Player Performance Visualization")
        st.caption("Detailed analysis of player contributions to team's attacking output")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("**Goals and Assists Distribution**")
            st.caption("Shows each player's balance between scoring goals (blue) and providing assists (orange). Players with both high goals and assists are most valuable.")
            fig = px.bar(
                player_df.head(10),
                x='player_name',
                y=['goals', 'assists'],
                labels={'player_name': 'Player', 'value': 'Count'},
                barmode='group',
                title="Goals vs Assists by Player"
            )
            fig.update_layout(
                xaxis_tickangle=-45,
                height=400
            )
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.markdown("**Goal Involvement Scatter Analysis**")
            st.caption("Bubble size represents total goal involvement. Players in top-right quadrant contribute most through both goals and assists. Larger bubbles = higher impact.")
            fig = px.scatter(
                player_df,
                x='goals',
                y='assists',
                size='goal_involvement',
                hover_data=['player_name'],
                labels={'goals': 'Goals', 'assists': 'Assists'},
                color='goal_involvement',
                color_continuous_scale='plasma',
                title="Player Goal Involvement Analysis"
            )
            fig.update_layout(height=400)
            st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No player data available for this team.")