from dagster import Definitions
from openalex_terminusdb.hackernews import hackernews_assets

definition = Definitions(
    sensors=[],
    jobs=[],
    assets=[
        *hackernews_assets,
    ],
    resources={},
)
