from pathlib import Path

import boto3
from botocore.exceptions import ClientError

from .pod import POD
from .utils import logger


class S3POD(POD):

    protocol = "s3"

    def __init__(
        self,
        path,
        netloc=None,
        profile=None,
        verify=True,
        client=None,
        key=None,
        secret=None,
        token=None,
    ):
        bucket, *parts = path.parts
        self.path = Path(*parts)
        self.bucket = Path(bucket)
        if client:
            self.client = client
        else:
            # Disable ssl verifications if asked. Please note that
            # boto will take "REQUESTS_CA_BUNDLE" env variable.
            client_kwargs = {
                "use_ssl": True if verify else None,
            }
            if netloc:
                # TODO support for https on custom endpoints
                # TODO document use of param: endpoint_url='http://127.0.0.1:5300'
                client_kwargs["endpoint_url"] = f"http://{netloc}"
            self.client = boto3.client(
                "s3",
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                # **client_kwargs, #FIXME
            )
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return S3POD(self.bucket / path, client=self.client)

    def ls(self, relpath=".", missing_ok=False, limit=None):
        logger.debug("LIST s3:///%s/%s %s", self.bucket, self.path, relpath)
        paginator = self.client.get_paginator("list_objects")
        prefix = str(self.path / relpath) + "/"
        cut = len(str(self.path)) + 1
        options = {
            "Bucket": str(self.bucket),
            "Prefix": prefix,
            "Delimiter": "/",
        }
        if limit is not None:
            options["PaginationConfig"] = {"MaxItems": limit}

        page_iterator = paginator.paginate(**options)
        names = []
        for page in page_iterator:
            common_prefixes = page.get("CommonPrefixes", [])
            contents = page.get("Contents", [])
            names.extend(item["Prefix"][cut:] for item in common_prefixes)
            names.extend(item["Key"][cut:] for item in contents)
        return names

    def read(self, relpath, mode="rb"):
        logger.debug("READ s3:///%s/%s %s", self.bucket, self.path, relpath)
        key = str(self.path / relpath)
        resp = self.client.get_object(Bucket=str(self.bucket), Key=key)
        return resp["Body"].read()

    def write(self, relpath, data, mode="wb"):
        if self.isfile(relpath):
            logger.debug("SKIP-WRITE s3:///%s/%s %s", self.bucket, self.path, relpath)
            return
        logger.debug("WRITE s3:///%s%s %s", self.bucket, self.path, relpath)
        key = str(self.path / relpath)
        response = self.client.put_object(
            Bucket=str(self.bucket),
            Body=data,
            Key=key,
        )
        assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
        return len(data)

    def isdir(self, relpath):
        return len(self.ls(relpath, limit=1)) > 0

    def isfile(self, relpath):
        key = str(self.path / relpath)
        try:
            _ = self.client.get_object(Bucket=str(self.bucket), Key=key)
        except ClientError as err:
            if err.response["Error"]["Code"] == "NoSuchKey":
                return False
            # Other kind of error, reraise
            raise
        return True

    def rm(self, relpath=".", recursive=False, missing_ok=False):
        logger.debug("REMOVE s3://%s/%s %s", self.bucket, self.path, relpath)
        key = str(self.path / relpath)
        _ = self.client.delete_object(
            Bucket=str(self.bucket),
            Key=key,
        )
        # TODO implement missing_ok

    def mv(self, from_path, to_path, missing_ok=False):
        orig = str(self.path / from_path)
        dest = str(self.path / to_path)
        logger.debug(
            "MOVE s3://%s/%s to s3://%s/%s", self.bucket, orig, self.bucket, dest
        )
        try:
            self.client.copy(
                {"Bucket": str(self.bucket), "Key": orig},
                str(self.bucket),
                dest,
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "404":
                if missing_ok:
                    return
                raise FileNotFoundError(f'Path "{orig}" not found')
            raise
