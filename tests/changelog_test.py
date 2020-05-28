from tempfile import TemporaryDirectory
from concurrent.futures import ThreadPoolExecutor

import pytest
import zarr

from baltic import Changelog
from baltic.utils import hexdigest


@pytest.yield_fixture(scope='function', params=[None, 'tmp'])
def grp(request):
    if request.param is None:
        yield zarr.group()
        return

    with TemporaryDirectory() as tdir:
        yield zarr.group(tdir)

def populate(changelog, datum):
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = 'Doe'
        info = f'{key} {timestamp} {author}'.encode()
        changelog.commit([info.decode()])


def test_commit(grp):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(grp)
    populate(changelog, datum)

    res = list(grp)
    assert len(res) == len(datum)

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))


def test_concurrent_commit(grp):
    datum = b'ham spam foo bar baz'.split()
    changelogs = [Changelog(grp) for _ in range(len(datum))]

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

    res = list(grp)
    assert len(res) == len(datum)

    # As we inserted datum in a random fashion we have no order
    # garantee
    expected = set(map(hexdigest, datum))
    for item in changelog.read():
        key, _ = item.split(' ', 1)
        expected.remove(key)
    assert not expected


def test_pack(grp):
    # Create 5 changeset in series
    datum = b'ham spam foo bar baz'.split()
    changelog = Changelog(grp)
    populate(changelog, datum)

    changelog.pack()

    # Read commits
    for data, expected in zip(changelog.read(), datum):
        assert data.startswith(hexdigest(expected))
