from itertools import chain
from pathlib import PurePosixPath

import boto3
import urllib3
from botocore.exceptions import ClientError

from .pod import POD
from .utils import logger


def silence_insecure_warning():
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
        """
        `path` must contains bucket name and sub-path in the bucket
        (separated by a `/`). `verify` set to False will disable ssl
        verifications. Please note that boto3 will also use
        "REQUESTS_CA_BUNDLE" env variable.
        """
        bucket, *parts = path.parts
        self.path = PurePosixPath(*parts)
        self.bucket = PurePosixPath(bucket)
        if client:
            self.client = client
        else:
            # TODO support for https on custom endpoints
            # TODO document use of param: endpoint_url='http://127.0.0.1:5300'
            if not verify:
                silence_insecure_warning()
            endpoint_url = f"http://{netloc}" if netloc else None
            session = boto3.session.Session(
                aws_access_key_id=key,
                aws_secret_access_key=secret,
                aws_session_token=token,
                profile_name=profile,
            )
            self.client = session.client(
                "s3", verify=verify, aws_session_token=token, endpoint_url=endpoint_url
            )
        super().__init__()

    def cd(self, *others):
        path = self.path.joinpath(*others)
        return S3POD(self.bucket / path, client=self.client)

    def ls_iter(self, prefix, limit=None, delimiter="/"):
        '''
        Iterable that yield lists of object keys
        '''
        logger.debug("LIST s3:///%s/%s", self.bucket, prefix)
        paginator = self.client.get_paginator("list_objects")
        options = {
            "Bucket": str(self.bucket),
            "Prefix": prefix,
        }
        if delimiter:
            options["Delimiter"] = delimiter
        if limit is not None:
            options["PaginationConfig"] = {"MaxItems": limit}

        page_iterator = paginator.paginate(**options)
        for page in page_iterator:
            # Extract pseudo-folder names
            common_prefixes = page.get("CommonPrefixes", [])
            yield [item["Prefix"].rstrip("/") for item in common_prefixes]
            # Extract keys (filenames)
            contents = page.get("Contents", [])
            yield [item["Key"] for item in contents]

    def ls(self, relpath=".", missing_ok=False, limit=None, delimiter="/"):
        '''
        The parameter `missing_ok` is not supported, it is present for
        compatibility reason with the other pods.
        '''
        prefix = str(self.path / relpath)
        prefix = "" if prefix in (".", "") else prefix + delimiter
        cut = len(prefix)
        it = self.ls_iter(prefix, limit=limit, delimiter=delimiter)
        return [item[cut:] for item in chain.from_iterable(it)]

    def read(self, relpath, mode="rb"):
        logger.debug("READ s3:///%s/%s %s", self.bucket, self.path, relpath)
        key = str(self.path / relpath)
        try:
            resp = self.client.get_object(Bucket=str(self.bucket), Key=key)
        except ClientError as err:
            if err.response["Error"]["Code"] == "NoSuchKey":
                raise FileNotFoundError(f'Key "{relpath}" not found')

        return resp["Body"].read()

    def write(self, relpath, data, mode="wb", force=False):
        if not force and self.isfile(relpath):
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
        prefix = str(self.path / relpath)
        if not recursive:
            if missing_ok:
                keys = [prefix]
            else:
                # We must make sure the key exists
                it = self.ls_iter(prefix, limit=2, delimiter="")
                keys = list(chain.from_iterable(it))
                if not keys:
                    raise FileNotFoundError(f"{relpath} not found")

            if len(keys) > 1:
                # We raise an OSError to mimic file based access
                raise OSError(f"{relpath} is not empty")
            self._delete_keys([prefix])
            return

        # Recursive case, we use ls_iter to loop on a potentially
        # large number of files
        prefix = "" if prefix in (".", "") else prefix + "/"
        files_found = False
        for chunk in self.ls_iter(prefix, delimiter=""):
            if not chunk:
                # can be empty
                continue
            files_found = True
            self._delete_keys(chunk)

        if not files_found and not missing_ok:
            raise FileNotFoundError(f"{relpath} not found")

    def _delete_keys(self, keys):
        try:
            _ = self.client.delete_objects(
                Bucket=str(self.bucket),
                Delete={
                    "Objects": [{"Key": k} for k in keys],
                    "Quiet": True,
                },
            )

            # TODO check for error in response
        except ClientError as err:
            if err.response["Error"]["Code"] != "MalformedXML":
                raise
            # As of version 2.2.6, Moto doesn't support correctly
            # delete_objects() calls, we fall back to delete_object()
            for key in keys:
                self.client.delete_object(
                    Bucket=str(self.bucket),
                    Key=key,
                )

    def mv(self, from_path, to_path, missing_ok=False):
        orig = str(self.path / from_path)
        dest = str(self.path / to_path)
        logger.debug(
            "MOVE s3://%s/%s to s3://%s/%s", self.bucket, orig, self.bucket, dest
        )
        try:
            # Copy to dest
            self.client.copy(
                {"Bucket": str(self.bucket), "Key": orig},
                str(self.bucket),
                dest,
            )
            # Delete orig
            self.client.delete_object(
                Bucket=str(self.bucket),
                Key=orig,
            )
        except ClientError as err:
            if err.response["Error"]["Code"] == "404":
                if missing_ok:
                    return
                raise FileNotFoundError(f'Path "{orig}" not found')
            raise
