from dagster import Definitions

from openalex_terminusdb.config import (
    get_s3_config,
    get_mongo_config,
    get_terminus_config,
)
from openalex_terminusdb.etl import etl_assets
from openalex_terminusdb.resources import (
    S3Resource,
    MongoResource,
    TerminusResource,
    ConfigurableGzippedNdJsonS3IOManager,
)

configured_s3_resource = S3Resource(**get_s3_config())
configured_mongo_resource = MongoResource(**get_mongo_config())
configured_terminus_resource = TerminusResource(**get_terminus_config())
configured_gzipped_ndjson_s3_io_manager = ConfigurableGzippedNdJsonS3IOManager(
    s3_resource=configured_s3_resource,
    s3_bucket="polyneme",
    s3_prefix="openalex_terminusdb_dagster",
)

defs = Definitions(
    sensors=[],
    jobs=[],
    assets=etl_assets,
    resources={
        "s3": configured_s3_resource,
        "mongo": configured_mongo_resource,
        "terminus": configured_terminus_resource,
        "gzipped_ndjson_s3_io_manager": configured_gzipped_ndjson_s3_io_manager,
    },
    schedules=[],
)
