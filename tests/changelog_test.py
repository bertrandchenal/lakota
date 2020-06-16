from pathlib import Path
from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import pytest
import fsspec

from baltic import Changelog
from baltic.utils import hexdigest


@pytest.yield_fixture(scope='function', params=[None, 'tmp'])
def fs(request):
    if request.param is None:
        yield fsspec.filesystem('memory')
    else:
        yield fsspec.filesystem('file')


@pytest.fixture(scope='function')
def path(fs):
    if isinstance(fs, fsspec.implementations.memory.MemoryFileSystem):
        # We use random path because fsspec always tries to reuse the same
        # in-memory instance
        yield Path(str(uuid4()))
    else:
        with TemporaryDirectory() as tdir:
            yield Path(tdir)


def populate(changelog, datum):
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        changelog.commit([info.decode()])


def test_commit(fs, path):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(fs, path)
    populate(changelog, datum)

    res = fs.ls(str(path))
    assert len(res) == len(datum)

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))


def test_concurrent_commit(fs, path):
    datum = b'ham spam foo bar baz'.split()
    changelogs = [Changelog(fs, path) for _ in range(len(datum))]
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

    res = fs.ls(str(path))
    assert len(res) == len(datum)

    # As we inserted datum in a random fashion we have no order
    # garantee
    expected = set(map(hexdigest, datum))
    for item in changelog.read():
        key, _ = item.split(' ', 1)
        expected.remove(key)
    assert not expected


def test_pack(fs, path):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(fs , path)
    populate(changelog, datum)

    changelog.pack()

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))
