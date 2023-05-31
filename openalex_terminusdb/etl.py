import json
from pathlib import Path

from dagster import asset, OpExecutionContext, load_assets_from_current_module
from terminusdb_client import GraphType

from openalex_terminusdb.config import get_terminus_config
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
    context.log.info(tdb.info())


@asset(io_manager_key="gzipped_ndjson_s3_io_manager")
def openalex_db_created(
    context: OpExecutionContext,
    terminus: TerminusResource,
):
    tdb = terminus.get_client()
    # XXX https://github.com/terminusdb/terminusdb-client-python/pull/401
    r = tdb._session.head(
        f"{tdb.api}/db/{tdb.team}/openalex",
        headers=tdb._default_headers,
        auth=tdb._auth(),
        allow_redirects=True,
    )
    if not r.status_code == 200:
        context.log.info("creating database")
        tdb.create_database("openalex", team=get_terminus_config()["team"])
    context.log.info(tdb.get_database("openalex"))
    return [tdb.get_database("openalex")]


@asset(io_manager_key="gzipped_ndjson_s3_io_manager")
def openalex_db_with_schema_set(
    context: OpExecutionContext,
    openalex_db_created: list[dict],
    terminus: TerminusResource,
):
    context.log.info(openalex_db_created)
    tdb = terminus.get_client()
    tdb.set_db("openalex")
    schema = json.loads(Path(__file__).parent.joinpath("schema.json").read_text())
    tdb.insert_document(schema, graph_type=GraphType.SCHEMA, full_replace=True)
    existing_classes = tdb.get_existing_classes()
    context.log.info(existing_classes)
    return [existing_classes]


etl_assets = load_assets_from_current_module(group_name="etl")
