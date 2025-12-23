import os
from dagster import Definitions, define_asset_job, AssetSelection
from dagster.asset import AssetExecutionContext
from .assets import (
    teams_data,
    fixtures_data,
    standings_data,
    scorers_data,
    dbt_transformations,
    dbt_resource,
)

pl_mvp_pipeline = define_asset_job(
    name="pl_mvp_pipeline",
    selection=AssetSelection.all(),
    description="Premier League MVP Pipeline: Runs all ingestion pipelines and dbt transformations",
)

defs = Definitions(
    assets=[
        teams_data,
        fixtures_data,
        standings_data,
        scorers_data,
        dbt_transformations,
    ],
    resources={"dbt": dbt_resource},
    jobs=[pl_mvp_pipeline],
)