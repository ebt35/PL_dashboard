import os
import streamlit as st

def home_page():
    st.title("Premier League Data Platform")
    st.caption("Comprehensive analytics and insights for the 2025/2026 Premier League season")

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
    Established in 1992, it has become one of the most watched and competitive football leagues in the world.
    
    This platform provides comprehensive data analytics for the **2025/2026 season**, including:
    - **Team Performance Metrics**: Points, goals, win rates, and league standings
    - **Player Statistics**: Goals, assists, and goal involvement
    - **Match Data**: Fixtures, results, and detailed match information
    - **Real-time Insights**: Interactive visualizations and KPI dashboards
    
    Navigate through the pages to explore detailed analytics and insights.
    """)
    st.divider()

    # Architecture pdf
    st.header("System Architecture")
    pdf_path = os.path.join(
        project_root,
        "docs",
        "Football_pipeline_architecture.pdf",
    )

    if os.path.exists(pdf_path):
        with open(pdf_path, "rb") as pdf_file:
            st.download_button(
                label="Download Architecture Document",
                data=pdf_file,
                file_name="Football_pipeline_architecture.pdf",
                mime="application/pdf",
            )
    else:
        st.info(
            "Architecture document not found. Ensure it exists in /docs."
        )
