# Streamlit Dashboard

Premier League Data Platform - Streamlit Dashboard

## Features

- **Homepage**: About the Premier League, system architecture
- **League Overview**: League-wide statistics, league table, top scorers, top assist providers and performance charts
- **Team Overview**: Team-specific KPIs and player statistics
- **Source Datasets**: Raw data source from API-Football

## KPIs Displayed

### Team KPIs
- Matches played
- Win
- Draw
- Loss
- Goal for
- Goal against
- Goal difference
- Total points
- Form
- Results & Fixtures
- Club information
- Core Team Performance Metrics
- Performance Benchmarking
- Squad Performance Metrics
- Disciplinary Records
  
### Player KPIs
- Goals
- Assists
- Goal involvement
- Goals per game
- Assists per game
- Disciplinary Records

## Usage

Run the dashboard:
```bash
cd dashboard_streamlit

uv run streamlit run app.py

or

uv run streamlit run dashboard_streamlit.app.py
```

The dashboard will open in your browser at `http://localhost:8501`

## Data Source

The dashboard reads from:
- `mart.mart_team_kpi` - Team KPIs
- `mart.mart_player_kpi` - Player KPIs

Make sure to run the Dagster pipeline first to populate these tables.

