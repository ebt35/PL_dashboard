import os
import streamlit as st

def home_page():
    st.title("Premier League Data Platform")
    st.caption(
        "Comprehensive analytics and insights for the 2025/2026 Premier League season"
    )

    project_root = os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", "..")
    )

    # Banner
    logo_path = os.path.join(project_root, "utils", "hero-banner.png")
    if os.path.exists(logo_path):
        st.image(logo_path, use_container_width=False)

    st.divider()

    # Premier League Description
    st.header("About the Premier League")
    st.markdown("""
    The **Premier League** is the top tier of English football, featuring 20 of the best clubs in England.
    Established in 1992, it has grown into one of the most watched and competitive football leagues in the world.

    This platform is designed for **football analysts, data-driven decision makers and enthusiasts**, providing
    comprehensive analytics for the **2025/2026 season**, including:

    - **Team Performance Metrics**: Points, goals, win rates, goal differences, and league standings  
    - **Player Statistics**: Goals, assists, goal involvement and individual contributions  
    - **Match Data**: Fixtures, results, and detailed match-level information  
    - **Real-time Insights**: Interactive visualizations, comparisons and KPI dashboards  

    Navigate through the platform to explore team-level and player-level performance, identify trends
    and gain deeper insight into the dynamics of the Premier League season.
    """)
    st.divider()

    # System Architecture
    st.header("System Architecture")
    st.markdown(
        "Overview of the data pipeline, API integration and dashboard architecture powering the platform."
    )

    pdf_path = os.path.join(
        project_root,
        "docs",
        "Football_pipeline_architecture.pdf",
    )

    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as pdf_file:
            st.download_button(
                label="View System Architecture",
                data=pdf_file,
                file_name="Football_pipeline_architecture.pdf",
                mime="application/pdf",
            )
    else:
        st.info(
            "Architecture document not found. Please ensure it exists in the /docs directory."
        )

