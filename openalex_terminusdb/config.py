from functools import lru_cache
import os
from typing import TypedDict

from pydantic import BaseModel


@lru_cache
def get_mongo_config():
    return {
        "host": os.getenv("MONGO_HOST"),
        "username": os.getenv("MONGO_USER"),
        "password": os.getenv("MONGO_PASS"),
    }


@lru_cache
def get_terminus_config():
    return {
        "server_url": os.getenv("TERMINUS_HOST"),
        "team": os.getenv("TERMINUS_TEAM"),
        "user": os.getenv("TERMINUS_USER"),
        "key": os.getenv("TERMINUS_PASS"),
    }


@lru_cache
def get_s3_config():
    return {
        "region_name": os.getenv("AWS_REGION_NAME"),
        "endpoint_url": os.getenv("AWS_ENDPOINT_URL"),
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
    }


@lru_cache
def get_s3_cdn_hostname():
    return os.getenv("S3_CDN_HOSTNAME")


class OpenalexSnapshotS3Config(BaseModel):
    bucket: str
    prefix: str


@lru_cache
def get_openalex_snapshot_s3_config():
    return OpenalexSnapshotS3Config(
        bucket=os.getenv("OPENALEX_SNAPSHOT_S3_BUCKET"),
        prefix=os.getenv("OPENALEX_SNAPSHOT_S3_PREFIX"),
    )
