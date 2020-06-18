from tempfile import TemporaryDirectory

import pytest
from moto import mock_s3

from baltic import POD


@pytest.yield_fixture(scope="function", params=["memory", "s3", "tmp"])
def pod(request):
    if request.param == "memory":
        yield POD.from_uri("memory://")

    elif request.param == "tmp":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f"file://{tdir}")

    elif request.param == "s3":
        # S3 tested with moto
        with mock_s3():
            uri = "s3://test-bucket/"
            pod = POD.from_uri(uri)
            # Make sur bucket exists
            pod.fs.mkdir("test-bucket")
            yield pod

    else:
        raise
