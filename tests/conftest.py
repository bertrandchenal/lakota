from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from moto import mock_s3

from jensen import POD


@pytest.yield_fixture(scope="function", params=["file", "memory", "s3", "cache"])
def pod(request):
    if request.param == "memory":
        yield POD.from_uri("memory://")

    elif request.param == "file":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f"file://{tdir}")

    elif request.param == "cache":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            pod = POD.from_uri(["memory://", f"file://{tdir}"])
            yield pod

    elif request.param == "s3":
        # S3 tested with moto
        with mock_s3():
            # Pick a random bucket name to overcome s3fs caching
            bucket = str(uuid4())
            uri = f"s3://{bucket}/"
            pod = POD.from_uri(uri)
            # Make sur bucket exists
            pod.fs.mkdir(bucket)
            yield pod

        # bucket = 'e8e90f41-5465-45fa-9486-570448278a8a'
        # uri = f"s3://{bucket}/"
        # pod = POD.from_uri(uri)
        # # pod.fs.mkdir(bucket)
        # yield pod

    else:
        raise
