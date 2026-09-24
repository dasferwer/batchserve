import os

import boto3
from botocore.exceptions import ClientError


def client():
    return boto3.client("s3", endpoint_url=os.environ["S3_ENDPOINT"], region_name="us-east-1")


def bucket():
    return os.environ.get("S3_BUCKET", "batches")


def upload(path, key):
    s3 = client()
    try:
        s3.head_bucket(Bucket=bucket())
    except ClientError as exc:
        if exc.response["ResponseMetadata"]["HTTPStatusCode"] != 404:
            raise
        try:
            s3.create_bucket(Bucket=bucket())
        except ClientError as race:
            if race.response["Error"]["Code"] not in {
                "BucketAlreadyOwnedByYou",
                "BucketAlreadyExists",
            }:
                raise
    s3.upload_file(str(path), bucket(), key)


def download(key, path):
    client().download_file(bucket(), key, str(path))
