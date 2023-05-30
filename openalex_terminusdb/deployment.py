from dagster import Definitions, define_asset_job, ScheduleDefinition, \
    load_assets_from_package_module

from openalex_terminusdb import etl
from openalex_terminusdb.config import (
    get_s3_config,
    get_mongo_config,
    get_terminus_config,
)
from openalex_terminusdb.etl import connections_okay
from openalex_terminusdb.resources import S3Resource, MongoResource, TerminusResource


configured_s3_resource = S3Resource(**get_s3_config())
configured_mongo_resource = MongoResource(**get_mongo_config())
configured_terminus_resource = TerminusResource(**get_terminus_config())

etl_assets = load_assets_from_package_module(
    etl,
    group_name="etl",
)

defs = Definitions(
    sensors=[],
    jobs=[],
    assets=etl_assets,
    resources={
        "s3": configured_s3_resource,
        "mongo": configured_mongo_resource,
        "terminus": configured_terminus_resource,
    },
    schedules=[],
)
