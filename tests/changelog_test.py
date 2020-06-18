from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest

from baltic import Changelog
from baltic import POD
from baltic.utils import hexdigest



@pytest.yield_fixture(scope='function', params=[None, 'tmp'])
def pod(request):
    if request.param is None:
        # We use random path to force fsspec to give us distinct
        # in-memory instances
        uuid = uuid4()
        yield POD.from_uri(f'memory://{uuid}')
    else:
        with TemporaryDirectory() as tdir:
            yield POD.from_uri(f'file://{tdir}')


def populate(changelog, datum):
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        changelog.commit([info.decode()])


def test_commit(pod):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(pod)
    populate(changelog, datum)

    res = pod.ls()
    assert len(res) == len(datum)

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))


def test_concurrent_commit(pod):
    datum = b'ham spam foo bar baz'.split()
    changelogs = [Changelog(pod) for _ in range(len(datum))]
    contents = []
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        contents.append(info)

    # Concurrent commits
    with ThreadPoolExecutor() as executor:
        futs = []
        for changelog, info in zip(changelogs, contents):
            f = executor.submit(changelog.commit, [info.decode()], _jitter=True)
            futs.append(f)
        executor.shutdown()

    for f in futs:
        assert not f.exception()

    res = pod.ls()
    assert len(res) == len(datum)

    # As we inserted datum in a random fashion we have no order
    # garantee
    expected = set(map(hexdigest, datum))
    for item in changelog.read():
        key, _ = item.split(' ', 1)
        expected.remove(key)
    assert not expected


def test_pack(pod):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(pod)
    populate(changelog, datum)

    changelog.pack()

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))
