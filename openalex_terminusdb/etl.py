from dagster import asset, OpExecutionContext, load_assets_from_current_module

from openalex_terminusdb.resources import S3Resource, MongoResource, TerminusResource


@asset
def connections_okay(
    context: OpExecutionContext,
    s3: S3Resource,
    mongo: MongoResource,
    terminus: TerminusResource,
):
    s3client = s3.get_client()
    context.log.info(s3client.list_buckets())

    mdb = mongo.get_client().get_database("openalex")
    context.log.info(mdb.list_collection_names())

    tdb = terminus.get_client()
    context.log.info(tdb.get_database("openalex_snapshot"))


etl_assets = load_assets_from_current_module(group_name="etl")
