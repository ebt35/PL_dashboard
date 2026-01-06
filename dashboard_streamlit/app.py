import streamlit as st
from pathlib import Path
import os
from views.home import home_page
from views.league_overview import league_overview
from views.team_overview import team_overview
from views.source_datasets import source_datasets


# =========================
# CSS LOADER (SINGLE SOURCE)
# =========================
def load_css(filename: str):
    css_path = Path(__file__).parent / "styles" / filename
    if css_path.exists():
        with open(css_path) as f:
            st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)


# Load base styles FIRST
load_css("main.css")


# =========================
# DARK MODE STATE
# =========================
if "dark_mode" not in st.session_state:
    st.session_state.dark_mode = False


with st.sidebar:
    st.markdown("### Theme")
    st.session_state.dark_mode = st.toggle(
        "Dark mode",
        value=st.session_state.dark_mode
    )


# Load dark mode styles AFTER base styles
if st.session_state.dark_mode:
    load_css("dark.css")




st.set_page_config(
    page_title="Premier League Data Platform",
    page_icon="⚽",
    layout="wide"
)

def main():
    st.sidebar.title("PREMIER LEAGUE DASHBOARD")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Home", "League Overview", "Team Overview", "Source Datasets"],
        index=0
    )

    if page == "Home":
        home_page()
    elif page == "League Overview":
        league_overview()
    elif page == "Team Overview":
        team_overview()
    elif page == "Source Datasets":
        source_datasets()

if __name__ == "__main__":
    main()
