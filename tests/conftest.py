from tempfile import TemporaryDirectory
from uuid import uuid4

import boto3
import pytest
from moto import mock_s3

from lakota import POD
from lakota.utils import settings

params = ["file", "memory", "s3", "memory+s3"]


@pytest.yield_fixture(scope="function", params=params)
def pod(request):
    if request.param == "memory":
        yield POD.from_uri("memory://")

    elif request.param == "file":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f"file://{tdir}")

    elif 's3' in request.param:
        # S3 tested with moto
        with mock_s3():
            # Pick a random bucket name to overcome s3fs caching
            bucket = str(uuid4())
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=bucket)
            s3_uri = f"s3://{bucket}/"
            if request.param == "s3":
                pod = POD.from_uri(s3_uri)
            elif request.param == "memory+s3":
                pod = POD.from_uri(["memory://", s3_uri])
            # Make sure bucket exists
            yield pod
    else:
        raise


@pytest.fixture(scope="function", params=[False, True])
def threaded(request):
    settings.threaded = request.param
    return request.param
