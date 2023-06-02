import json
from collections import defaultdict
from pathlib import Path

from mypy_boto3_s3.client import S3Client
from dagster import (
    asset,
    OpExecutionContext,
    load_assets_from_current_module,
    Config,
    Failure,
    Output,
)
from terminusdb_client import GraphType, WOQLClient, WOQLQuery as WQ
from toolz import get_in, keyfilter, assoc, dissoc

from openalex_terminusdb.config import (
    get_terminus_config,
    get_openalex_snapshot_s3_config,
)
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


class SnapshotEntityFieldConfig(Config):
    entity_collection_name: str
    field_path: str


@asset(io_manager_key="gzipped_ndjson_s3_io_manager")
def openalex_snapshot_entity_field_values(
    context: OpExecutionContext,
    config: SnapshotEntityFieldConfig,
    s3: S3Resource,
):

    s3cfg = get_openalex_snapshot_s3_config()
    bucket = s3cfg.bucket
    entity_collections = ["authors", "concepts", "institutions", "venues", "works"]
    if config.entity_collection_name not in entity_collections:
        raise Failure(
            description=f"{config.entity_collection_name} not in {entity_collections}."
        )
    prefix = f"{s3cfg.prefix}/data/{config.entity_collection_name}/"
    field_values = []
    keys = s3.list_object_keys(bucket=bucket, prefix=prefix)
    keys = [k for k in keys if k.endswith(".gz")]
    for i, k in enumerate(keys, start=1):
        context.log.info(
            f"processing {k} (file {i} of {len(keys)} for {config.entity_collection_name})..."
        )
        docs = s3.load_gzipped_ndjson_object(bucket=bucket, key=k)
        for doc in docs:
            value = get_in(config.field_path.split("."), doc)
            field_values.append(value)
    return Output(
        [
            {
                config.field_path: field_values,
                "entity_collection_name": config.entity_collection_name,
            }
        ],
        metadata={
            "field_path": config.field_path,
            "entity_collection_name": config.entity_collection_name,
        },
    )


class SourceIdConfig(Config):
    source_id: str = "https://openalex.org/S24807848"  # Physical Review Letters


def pick(allowlist, d):
    return keyfilter(lambda k: k in allowlist, d)


def iri_from_class_and_id(tdb: WOQLClient, id_: str, cls: str):
    result = tdb.query(
        WQ()
        .triple("v:Id", "rdf:type", f"@schema:{cls}")
        .triple("v:Id", "@schema:id", WQ().string(id_))
    )
    return result["bindings"][0]["Id"]


@asset
def ingest_source_and_friends_by_id(
    context: OpExecutionContext,
    config: SourceIdConfig,
    s3: S3Resource,
    mongo: MongoResource,
    terminus: TerminusResource,
):
    tdb = terminus.get_client()
    tdb.set_db(dbid="openalex")
    context.log.info("Getting class frames...")
    framed_keys = {
        cls: [k for k in tdb.get_class_frame(cls).keys() if not k.startswith("@")]
        for cls in [
            "Work",
            "Institution",
            "Author",
            "Concept",
            "Source",
            "Identifiers",
            "Authorship",
            "ScoredConcept",
            "Location",
            "OpenAccess",
        ]
    }

    def pick_only_framed_for_field(doc, field, cls, multi=False):
        if doc[field] is None:
            return dissoc(doc, field)
        elif not multi:
            if isinstance(doc[field], str):
                return doc

            doc[field]["@type"] = cls
            return assoc(doc, field, pick(framed_keys[cls], doc[field]))
        else:
            new_field = []
            for elt in doc[field]:
                if isinstance(elt, str):
                    new_field.append(elt)
                else:
                    new_elt = pick(framed_keys[cls], elt)
                    new_field.append(assoc(new_elt, "@type", cls))
            return assoc(doc, field, new_field)

    projection = {
        cls: ({k: 1 for k in keys} | {"_id": 0}) for cls, keys in framed_keys.items()
    }
    mdb = mongo.get_client().get_database("openalex")

    context.log.info(f"Processing works for Source {config.source_id}...")
    for work_doc in mdb.works.find(
        {"locations.source.id": config.source_id}, projection["Work"]
    ):
        context.log.info(f"Processing Work {work_doc['id']} authorships...")
        for i, authorship_subdoc in enumerate(work_doc["authorships"]):
            context.log.info(
                f"Processing Work {work_doc['id']} authorship institutions..."
            )
            for j, institution_subdoc in enumerate(authorship_subdoc["institutions"]):
                institution_doc = mdb.institutions.find_one(
                    {"id": institution_subdoc["id"]}, projection["Institution"]
                )
                # XXX for id:"https://openalex.org/W2041029907",
                #     authorships.0.institutions.id is None (!) (in the 2023-03-28 snapshot)
                if institution_doc is None:
                    institution_doc = (
                        {"id": institution_subdoc["id"]}
                        if institution_subdoc["id"] is not None
                        else {"id": "https://openalex.org/INull"}
                    )

                institution_doc |= {"@type": "Institution"}
                tdb.update_document(institution_doc)
                institution_iri = iri_from_class_and_id(
                    tdb, institution_doc["id"], "Institution"
                )
                context.log.info(institution_iri)
                work_doc["authorships"][i]["institutions"][j] = institution_iri

            context.log.info(f"Processing Work {work_doc['id']} authorship author...")
            author_doc = mdb.authors.find_one(
                {"id": authorship_subdoc["author"]["id"]}, projection["Author"]
            )
            if author_doc is None:
                author_doc = (
                    {"id": authorship_subdoc["author"]["id"]}
                    if authorship_subdoc["author"]["id"] is not None
                    else {"id": "https://openalex.org/ANull"}
                )

            author_doc |= {"@type": "Author"}
            if "last_known_institution" in author_doc:
                author_doc = pick_only_framed_for_field(
                    author_doc,
                    "last_known_institution",
                    "Institution",
                )
            tdb.update_document(author_doc)
            author_iri = iri_from_class_and_id(tdb, author_doc["id"], "Author")
            context.log.info(author_iri)
            work_doc["authorships"][i]["author"] = author_iri

        context.log.info(f"Processing Work {work_doc['id']} concepts...")
        for i, concept_subdoc in enumerate(work_doc["concepts"]):
            concept_doc = mdb.concepts.find_one(
                {"id": concept_subdoc["id"]}, projection["Concept"]
            )
            if concept_doc is None:
                concept_doc = (
                    {"id": concept_subdoc["id"]}
                    if concept_subdoc["id"] is not None
                    else {"id": "https://openalex.org/CNull"}
                )
            concept_doc |= {"@type": "Concept"}
            if "ancestors" in concept_doc:
                concept_doc = pick_only_framed_for_field(
                    concept_doc, "ancestors", "Concept", multi=True
                )
                concept_doc = pick_only_framed_for_field(
                    concept_doc, "related_concepts", "ScoredConcept", multi=True
                )
            tdb.update_document(concept_doc)
            concept_iri = iri_from_class_and_id(tdb, concept_doc["id"], "Concept")
            context.log.info(concept_iri)
            work_doc["concepts"][i] = concept_iri

        context.log.info(f"Processing Work {work_doc['id']} locations...")
        for i, location_subdoc in enumerate(work_doc["locations"]):
            if location_source_subdoc := location_subdoc.get("source"):
                context.log.info(f'OpenAlex URL: {location_source_subdoc["id"]}')
                source_doc = mdb.sources.find_one(
                    {"id": location_source_subdoc["id"]}, projection["Source"]
                )
                # XXX id:"https://openalex.org/S4306400194" isn't in Mongo, but it's online.
                #     Treating ETL for such sources like `related_works` and `referenced_works`,
                #     i.e. "id-only-okay records", for now.
                if source_doc is None:
                    source_doc = (
                        {"id": location_source_subdoc["id"]}
                        if location_source_subdoc["id"] is not None
                        else {"id": "https://openalex.org/SNull"}
                    )

                source_doc |= {"@type": "Source"}
                tdb.update_document(source_doc)
                source_iri = iri_from_class_and_id(tdb, source_doc["id"], "Source")
                context.log.info(source_iri)
                work_doc["locations"][i]["source"] = source_iri

        context.log.info(f"Processing Work {work_doc['id']} itself for insertion...")

        work_doc |= {"@type": "Work"}
        if work_doc.get("primary_location", {}).get("source"):
            work_doc["primary_location"]["source"] = iri_from_class_and_id(
                tdb, work_doc["primary_location"]["source"]["id"], "Source"
            )
        if work_doc.get("best_oa_location", {}).get("source"):
            work_doc["best_oa_location"]["source"] = iri_from_class_and_id(
                tdb, work_doc["best_oa_location"]["source"]["id"], "Source"
            )

        for (field, cls, multi) in [
            ("ids", "Identifiers", False),
            ("authorships", "Authorship", True),
            ("concepts", "ScoredConcept", True),
            ("locations", "Location", True),
            ("primary_location", "Location", False),
            ("best_oa_location", "Location", False),
            ("open_access", "OpenAccess", False),
        ]:
            work_doc = pick_only_framed_for_field(work_doc, field, cls, multi=multi)

        def ensure_loc_type_hints(loc):
            loc = pick(framed_keys["Location"], loc)
            if isinstance(loc, dict):
                loc["@type"] = "Location"
            return loc

        for i, loc in enumerate(work_doc["locations"]):
            work_doc["locations"][i] = ensure_loc_type_hints(loc)
        if "primary_location" in work_doc:
            work_doc["primary_location"] = ensure_loc_type_hints(
                work_doc["primary_location"]
            )
        if "best_oa_location" in work_doc:
            work_doc["best_oa_location"] = ensure_loc_type_hints(
                work_doc["best_oa_location"]
            )

        for works_array_field in ["related_works", "referenced_works"]:
            context.log.info(f"Upserting Work {work_doc['id']} {works_array_field}...")
            if work_doc.get(works_array_field):
                tdb.update_document(
                    [
                        {"id": work_id, "@type": "Work"}
                        for work_id in work_doc[works_array_field]
                    ]
                )
                work_doc[works_array_field] = [
                    d["Work"]
                    for d in tdb.query(
                        WQ().woql_or(
                            *[
                                WQ().triple(
                                    "v:Work", "@schema:id", WQ().string(work_id)  # noqa
                                )
                                for work_id in work_doc[works_array_field]
                            ]
                        )
                    )["bindings"]
                ]

        context.log.info(f"Upserting Work {work_doc['id']}...")
        context.log.info(work_doc)
        tdb.update_document(work_doc)


etl_assets = load_assets_from_current_module(group_name="etl")
