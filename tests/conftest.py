import time
from pathlib import Path
from shutil import rmtree
from subprocess import DEVNULL, Popen
from tempfile import TemporaryDirectory
from uuid import uuid4

import pytest
import requests
from botocore.session import Session
from moto import mock_s3

from lakota import POD
from lakota.utils import settings

http_uri = "http://127.0.0.1:8081/some_prefix/test_repo/"
s3_netloc = "127.0.0.1:8082"


def http_reset():
    params = {
        "recursive": "true",
        "path": '.',
    }
    resp = requests.post(http_uri + "rm", params=params)
    if resp.status_code not in (200, 404):
        resp.raise_for_status()

@pytest.fixture(scope="session")
def http_server():
    # Start http server
    with TemporaryDirectory() as tdir:
        proc = Popen(
            ["lakota", "serve", "-w", http_uri, f"/ file:///{tdir}"],
            stderr=DEVNULL,
            stdout=DEVNULL,
        )
        time.sleep(1)
        with proc:
            yield http_reset
            proc.kill()


def s3_reset(client):
    requests.post("http://127.0.0.1:8082/moto-api/reset")
    # We always create a fresh bucket to avoid caching issue # XXX still needed ?
    s3_bucket_id = str(uuid4())
    client.create_bucket(Bucket=s3_bucket_id)
    return s3_bucket_id


@pytest.fixture(scope="session")
def moto_server():
    # Start moto server
    proc = Popen(["moto_server", "s3", "-p", "8082"], stderr=DEVNULL, stdout=DEVNULL)
    time.sleep(1)
    with mock_s3(), proc:
        # Make sure bucket exists
        session = Session()
        client = session.create_client("s3", endpoint_url=f"http://{s3_netloc}")
        yield lambda: s3_reset(client)
        proc.kill()


params = [
    "file",
    "s3",
    "memory",
    "memory+s3",
    "http",
]  # TODO "ssh"


@pytest.fixture(scope="function", params=params)
def pod(request, http_server, moto_server):
    if request.param == "memory":
        yield POD.from_uri("memory://")

    elif request.param == "file":
        # Local filesytem
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f"file:///{tdir}")

    elif request.param == "http":
        http_server()  # Clear server pod
        pod = POD.from_uri(http_uri)
        yield pod

    elif "s3" in request.param:
        s3_bucket_id = moto_server()  # Clear moto bucket
        s3_uri = f"s3://{s3_netloc}/{s3_bucket_id}/"
        if request.param == "s3":
            pod = POD.from_uri(s3_uri)
        elif request.param == "memory+s3":
            pod = POD.from_uri(["memory://", s3_uri])
        yield pod

    else:
        raise


@pytest.fixture(scope="function", params=[False, True])
def threaded(request):
    settings.threaded = request.param
    return request.param
