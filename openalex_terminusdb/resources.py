import gzip
import json
from io import BytesIO
from typing import List

import boto3
import botocore
from dagster import (
    ConfigurableResource,
    ConfigurableIOManager,
    OutputContext,
    InputContext,
    MetadataValue,
)
from pymongo import MongoClient
from terminusdb_client import Client as TerminusClient

from openalex_terminusdb.config import get_s3_cdn_hostname


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

    def get_client(self):
        return MongoClient(
            host=self.host,
            username=self.username,
            password=self.password,
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


class ConfigurableGzippedNdJsonS3IOManager(ConfigurableIOManager):
    """Store and load gzipped newline-delimited JSON files (*.ndjson.gz) from S3."""

    s3_resource: S3Resource
    s3_bucket: str
    s3_prefix: str

    def _get_key(self, context) -> str:
        return "/".join([self.s3_prefix] + context.asset_key.path) + ".ndjson.gz"

    def handle_output(self, context: OutputContext, obj: list[dict]):
        _f = BytesIO()
        s3client = self.s3_resource.get_client()
        with gzip.open(_f, "wb") as f:
            for line in obj:
                f.write((json.dumps(line) + "\n").encode("utf-8"))
        s3client.put_object(
            Bucket=self.s3_bucket,
            Key=self._get_key(context),
            Body=_f.getvalue(),
            ACL="private",
        )
        url_expires_in_days = 7
        url = s3client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": self.s3_bucket,
                "Key": self._get_key(context),
            },
            ExpiresIn=url_expires_in_days * 24 * 60 * 60,
        )
        url = "/".join([f"https://{get_s3_cdn_hostname()}"] + url.split("/")[3:])

        context.add_output_metadata(
            {
                "s3_path": f"s3://{self.s3_bucket}/{self._get_key(context)}",
                "presigned_url": MetadataValue.url(url),
                "url_expires_in_days": url_expires_in_days,
            }
        )

    def load_input(self, context: InputContext):
        response = self.s3_resource.get_client().get_object(
            Bucket=self.s3_bucket,
            Key=self._get_key(context),
        )
        content = response["Body"].read()
        return [
            json.loads(line)
            for line in gzip.decompress(content).decode("utf-8").strip().split("\n")
        ]
