import streamlit as st

from views.home import home_page
from views.league_overview import league_overview
from views.team_overview import team_overview
from views.source_datasets import source_datasets

st.set_page_config(
    page_title="Premier League Data Platform",
    page_icon="⚽",
    layout="wide"
)

def main():
    st.sidebar.title("Navigation")
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
