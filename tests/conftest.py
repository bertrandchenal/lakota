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
    "memory",
    "memory+s3",
    "http",
]  # TODO "ssh"


@pytest.fixture(scope="function", params=params)
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
        web_uri = f"http://127.0.0.1:{port}/some_prefix/test_repo"
        with TemporaryDirectory() as tdir:
            proc = Popen(
                ["lakota", "-r", tdir, "serve", web_uri],
                stderr=DEVNULL,
                stdout=DEVNULL,
            )  # TODO launch only one process and clear repo between tests (same with moto)
            time.sleep(2)
            with proc:
                pod = POD.from_uri(web_uri)
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
                pod = POD.from_uri("memory://+" + s3_uri)

            yield pod
            proc.kill()

    else:
        raise


@pytest.fixture(scope="function", params=[False, True])
def threaded(request):
    settings.threaded = request.param
    return request.param
