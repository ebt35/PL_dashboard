# import os
# import sys

# project_root = os.path.dirname(os.path.abspath(__file__))
# if project_root not in sys.path:
#     sys.path.insert(0, project_root)

# from dagster import Definitions, define_asset_job, AssetSelection

# # Import from local orchestration package
# import orchestration.assets as orchestration_assets

# teams_data = orchestration_assets.teams_data
# fixtures_data = orchestration_assets.fixtures_data
# standings_data = orchestration_assets.standings_data
# scorers_data = orchestration_assets.scorers_data
# players_data = orchestration_assets.players_data
# dbt_transformations = orchestration_assets.dbt_transformations

# pl_mvp_pipeline = define_asset_job(
#     name="pl_mvp_pipeline",
#     selection=AssetSelection.all(),
#     description="Premier League MVP Pipeline: Runs all ingestion pipelines and dbt transformations",
# )

# defs = Definitions(
#     assets=[
#         teams_data,
#         fixtures_data,
#         standings_data,
#         scorers_data,
#         players_data,
#         dbt_transformations,
#     ],
#     jobs=[pl_mvp_pipeline],
# )

import os
import sys

project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    in_process_executor,
)

from orchestration.assets import (
    teams_data,
    fixtures_data,
    standings_data,
    scorers_data,
    players_data,
    dbt_transformations,
)

pl_mvp_pipeline = define_asset_job(
    name="pl_mvp_pipeline",
    selection=AssetSelection.all(),
    description="Premier League MVP Pipeline",
)

defs = Definitions(
    assets=[
        teams_data,
        fixtures_data,
        standings_data,
        scorers_data,
        players_data,
        dbt_transformations,
    ],
    jobs=[pl_mvp_pipeline],
    executor=in_process_executor,
)
