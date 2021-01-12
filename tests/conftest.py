import time
from subprocess import DEVNULL, Popen
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
from botocore.session import Session
from moto import mock_s3

from lakota import POD
from lakota.utils import settings

params = [
    "file",
    "s3",
    "memory+s3",
    "memory",
    "http",
]  # "ssh"


@pytest.yield_fixture(scope="function", params=params)
def pod(request):
    if request.param == "memory":
        yield POD.from_uri("memory://")

    elif request.param == "file":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f"file:///{tdir}")

    elif request.param == "http":
        # Start moto server
        port = "8081"
        netloc = f"127.0.0.1:{port}"
        with TemporaryDirectory() as tdir:
            proc = Popen(
                ["lakota", "-vv", "-r", tdir, "serve", netloc],
                stderr=DEVNULL,
                stdout=DEVNULL,
            )
            time.sleep(1)
            with proc:
                pod = POD.from_uri(f"http://{netloc}")
                yield pod
                proc.terminate()

    elif "s3" in request.param:
        # Start moto server
        port = "8082"
        netloc = f"127.0.0.1:{port}"
        proc = Popen(["moto_server", "s3", "-p", port], stderr=DEVNULL, stdout=DEVNULL)
        time.sleep(1)

        with mock_s3(), proc:
            # Pick a random bucket name to overcome s3fs caching
            bucket = str(uuid4())
            # Make sure bucket exists
            session = Session()
            client = session.create_client("s3", endpoint_url=f"http://{netloc}")
            client.create_bucket(Bucket=bucket)
            s3_uri = f"s3://{netloc}/{bucket}/"
            if request.param == "s3":
                pod = POD.from_uri(s3_uri)
            elif request.param == "memory+s3":
                pod = POD.from_uri("memory://" + s3_uri)

            yield pod
            proc.kill()

    else:
        raise


@pytest.fixture(scope="function", params=[False, True])
def threaded(request):
    settings.threaded = request.param
    return request.param
