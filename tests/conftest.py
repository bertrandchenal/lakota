import time
from subprocess import DEVNULL, Popen
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from botocore.session import Session
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
        # Start moto server
        port = '8888'
        endpoint_url = f"http://127.0.0.1:{port}/"
        proc = Popen(["moto_server", "s3", "-p", port], stderr=DEVNULL, stdout=DEVNULL)
        time.sleep(0.1)

        try:
            with mock_s3():
                # Pick a random bucket name to overcome s3fs caching
                bucket = str(uuid4())
                # Make sure bucket exists
                session = Session()
                client = session.create_client('s3', endpoint_url=endpoint_url)
                client.create_bucket(Bucket=bucket)
                s3_uri = f"s3://{bucket}/"
                if request.param == "s3":
                    pod = POD.from_uri(s3_uri, endpoint_url=endpoint_url)
                elif request.param == "memory+s3":
                    pod = POD.from_uri(["memory://", s3_uri], endpoint_url=endpoint_url)

                yield pod

        finally:
            # Stop server
            proc.kill()
            proc.wait()

    else:
        raise


@pytest.fixture(scope="function", params=[False, True])
def threaded(request):
    settings.threaded = request.param
    return request.param
