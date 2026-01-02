# dbt Football Pipeline

## Setup

1. Install dbt-duckdb:
   ```bash
   uv add dbt-duckdb
   ```

2. Set environment variable (optional):
   ```bash
   export DBT_DUCKDB_PATH=duckdb/football.duckdb
   ```

3. Run dbt:
   ```bash
   cd dbt
   uv run dbt debug
   uv run dbt run
   uv run dbt test
   ```

## Models

### Source Layer (src)
- `src_teams` - Raw teams data
- `src_fixtures` - Raw fixtures data
- `src_standings` - Raw standings data
- `src_scorers` - Raw scorers data

### Staging Layer (stg)
- `stg_teams` - Cleaned teams data
- `stg_fixtures` - Cleaned fixtures data with completion status
- `stg_standings` - Cleaned standings data with renamed columns
- `stg_scorers` - Cleaned scorers data with calculated goal involvement
- `stg_players` - Cleaned pleyers data with calculated goal involvement

### Mart Layer (mart)
- `mart_team_kpi` - Team KPIs (matches_played, total_points, goals_scored, goals_conceded)
- `mart_player_kpi` - Player KPIs (goals, assists, goal_involvement)
