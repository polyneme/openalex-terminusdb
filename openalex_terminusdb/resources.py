import boto3
import botocore
from dagster import ConfigurableResource
from pymongo import MongoClient
from terminusdb_client import Client as TerminusClient

from openalex_terminusdb.config import (
    get_s3_config,
    get_mongo_config,
    get_terminus_config,
)


class TerminusResource(ConfigurableResource):
    server_url: str
    team: str
    user: str
    key: str

    def get_client(self):
        _client = TerminusClient(server_url=self.server_url)
        _client.connect(team=self.team, user=self.user, key=self.key)
        return _client


class MongoResource(ConfigurableResource):
    host: str
    username: str
    password: str
    authSource: str = "admin"
    tls: bool = True

    def get_client(self):
        return MongoClient(
            host=self.host,
            username=self.username,
            password=self.password,
            authSource=self.authSource,
            tls=self.tls,
        )


class S3Resource(ConfigurableResource):
    region_name: str = "nyc3"
    endpoint_url: str = "https://nyc3.digitaloceanspaces.com"
    aws_access_key_id: str
    aws_secret_access_key: str

    def get_client(self):
        session = boto3.session.Session()
        return session.client(
            "s3",
            config=botocore.config.Config(s3={"addressing_style": "virtual"}),
            region_name=self.region_name,
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )
