from dagster import Definitions, define_asset_job, ScheduleDefinition

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

defs = Definitions(
    sensors=[],
    jobs=[],
    assets=[connections_okay],
    resources={
        "s3": configured_s3_resource,
        "mongo": configured_mongo_resource,
        "terminus": configured_terminus_resource,
    },
    schedules=[],
)
