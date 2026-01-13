import streamlit as st
import pandas as pd
from utils.plotly_theme import apply_dark_plotly
from data.queries import get_team_kpis, get_player_kpis, get_last_results, get_next_fixtures


def player_overview():
    st.title("Player Overview")
    st.caption("Filter players by team or search directly to view detailed information")

    player_df = get_player_kpis()

    #Filter
    col_team, col_player, col_search = st.columns([2, 2, 3])

    with col_team:
        teams = ["All Teams"] + sorted(
            player_df["team_name"].dropna().unique().tolist()
        )
        selected_team = st.selectbox("Team", teams)

    if selected_team != "All Teams":
        filtered_df = player_df[player_df["team_name"] == selected_team]
    else:
        filtered_df = player_df.copy()

    with col_player:
        players = (
            filtered_df[["player_id", "player_name"]]
            .dropna()
            .sort_values("player_name")
        )

        player_options = ["All Players"] + players["player_id"].tolist()

        selected_player_id = st.selectbox(
            "Player",
            player_options,
            format_func=lambda pid: (
                "All Players"
                if pid == "All Players"
                else players.loc[
                    players["player_id"] == pid, "player_name"
                ].values[0]
            )
        )

    with col_search:
        search_query = st.text_input(
            "Search player",
            placeholder="Type player name…"
        )

    #Search player
    if search_query:
        search_match = player_df[
            player_df["player_name"]
            .str.contains(search_query, case=False, na=False)
        ]

        if not search_match.empty:
            selected_player_id = search_match.iloc[0]["player_id"]

    #Player profile
    if selected_player_id == "All Players":
        st.info("Select a player or search by name to view profile ")
        return

    player = player_df[
        player_df["player_id"] == selected_player_id
    ].iloc[0]

    st.divider()
    st.subheader("Player Profile")

    #Player card
    col_img, col_info = st.columns([1, 6])

    with col_img:
        if pd.notna(player.get("player_photo")):
            st.image(player["player_photo"], width=180)

    with col_info:
        st.markdown(f"""
        <div class="content-card" style="max-width:300px; margin-top:0px;">
            <h4>{player['player_name']}</h4>
            <p>
            <strong>Age:</strong> {player.get('player_age', '–')}<br>
            <strong>Nationality:</strong> {player['player_nationality']}<br>
            <strong>Team:</strong> {player['team_name']}<br>
            <strong>Position:</strong> {player['games_position']}
            </p>
        </div>
        """, unsafe_allow_html=True)
    
    st.divider()

    #Performance&usage
    col_perf, col_usage = st.columns([1, 4])

    with col_perf:
        st.markdown(f"""
        <div class="content-card compact-card">
            <h5>Performance</h4>
            <p>
            <strong>Goals:</strong> {player['goals']}<br>
            <strong>Assists:</strong> {player['assists']}<br>
            <strong>Goals / Game:</strong> {player['goals_per_game']}<br>
            <strong>Assists / Game:</strong> {player['assists_per_game']}<br>
            <strong>Goal Involvement:</strong> {player['goal_involvement']}
            </p>
        </div>
        """, unsafe_allow_html=True)

    with col_usage:
        st.markdown(f"""
        <div class="content-card compact-card">
            <h5>Usage</h5>
            <p>
            <strong>Appearances:</strong> {player['games_appearances']}<br>
            <strong>Minutes:</strong> {player['games_minutes']}
            </p>
        </div>
        """, unsafe_allow_html=True)
