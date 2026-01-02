# Dagster Orchestration

This directory contains the Dagster orchestration setup for the Premier League Data Platform.

## Structure

- `assets.py`: Defines all Dagster assets (ingestion pipelines and dbt transformations)
- `definitions.py`: Defines the Dagster repository with assets, resources, and jobs

## Job

### `pl_mvp_pipeline`

A single job that runs:
1. All dlt ingestion pipelines (teams, fixtures, standings, scorers)
2. dbt transformations (src, stg, mart layers)

## Usage

1. Start Dagster UI:
   ```bash
   dagster dev -m dagster_defs
   ```
   
   Or from project root:
   ```bash
   dagster dev
   ```

2. Navigate to http://localhost:3000

3. In the UI:
   - Go to "Jobs" tab
   - Select `pl_mvp_pipeline`
   - Click "Materialize" to run the pipeline manually

## Pipeline Flow

```
teams_data → fixtures_data → standings_data → scorers_data → players_data → dbt_transformations
```

All ingestion assets run first, then dbt transformations run on the ingested data.

