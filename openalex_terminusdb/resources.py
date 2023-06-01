import gzip
import json
from io import BytesIO
from typing import List, Literal

import boto3
import botocore
from dagster import (
    ConfigurableResource,
    ConfigurableIOManager,
    OutputContext,
    InputContext,
    MetadataValue,
)
from mypy_boto3_s3 import S3Client
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

    def list_object_keys(self, bucket: str, prefix: str) -> list[str]:
        client: S3Client = self.get_client()
        is_truncated = True
        object_keys = []
        continuation_token = None
        while is_truncated:
            kwargs = {"Bucket": bucket, "Prefix": prefix}
            if continuation_token:
                kwargs["ContinuationToken"] = continuation_token
            response = client.list_objects_v2(**kwargs)
            for obj in response["Contents"]:
                object_keys.append(obj["Key"])
            is_truncated = response["IsTruncated"]
            continuation_token = (
                response["NextContinuationToken"] if is_truncated else None
            )
        return object_keys

    def load_gzipped_ndjson_object(self, bucket: str, key: str):
        client: S3Client = self.get_client()
        response = client.get_object(
            Bucket=bucket,
            Key=key,
        )
        content = response["Body"].read()
        return [
            json.loads(line)
            for line in gzip.decompress(content).decode("utf-8").strip().split("\n")
        ]

    def put_gzipped_ndjson_object(
        self,
        bucket: str,
        key: str,
        obj: list[dict],
        acl: Literal["private", "public-read"] = "private",
    ):
        client: S3Client = self.get_client()
        _f = BytesIO()
        n_lines = len(obj)
        with gzip.open(_f, "wb") as f:
            for i, line in enumerate(obj, start=1):
                f.write(
                    (json.dumps(line) + ("\n" if i < n_lines else "")).encode("utf-8")
                )
        client.put_object(
            Bucket=bucket,
            Key=key,
            Body=_f.getvalue(),
            ACL=acl,
        )

    def generate_presigned_url(self, bucket: str, key: str, expires_in_days: int = 7):
        client: S3Client = self.get_client()
        url = client.generate_presigned_url(
            ClientMethod="get_object",
            Params={
                "Bucket": bucket,
                "Key": key,
            },
            ExpiresIn=expires_in_days * 24 * 60 * 60,
        )
        return "/".join([f"https://{get_s3_cdn_hostname()}"] + url.split("/")[3:])


class ConfigurableGzippedNdJsonS3IOManager(ConfigurableIOManager):
    """Store and load gzipped newline-delimited JSON files (*.ndjson.gz) from S3."""

    s3_resource: S3Resource
    s3_bucket: str
    s3_prefix: str

    def _get_key(self, context) -> str:
        return "/".join([self.s3_prefix] + context.asset_key.path) + ".ndjson.gz"

    def handle_output(self, context: OutputContext, obj: list[dict]):
        _f = BytesIO()
        self.s3_resource.put_gzipped_ndjson_object(
            bucket=self.s3_bucket, key=self._get_key(context), obj=obj
        )
        url_expires_in_days = 7
        url = self.s3_resource.generate_presigned_url(
            bucket=self.s3_bucket,
            key=self._get_key(context),
            expires_in_days=url_expires_in_days,
        )
        context.add_output_metadata(
            {
                "s3_path": f"s3://{self.s3_bucket}/{self._get_key(context)}",
                "presigned_url": MetadataValue.url(url),
                "url_expires_in_days": url_expires_in_days,
            }
        )

    def load_input(self, context: InputContext):
        return self.s3_resource.load_gzipped_ndjson_object(
            bucket=self.s3_bucket, key=self._get_key(context)
        )
