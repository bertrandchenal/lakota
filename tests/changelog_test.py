from concurrent.futures import ThreadPoolExecutor

from lakota import Changelog
from lakota.changelog import Commit, phi
from lakota.utils import hexdigest


def populate(changelog, datum):
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = "Doe"
        info = f"{key} {timestamp} {author}"
        changelog.commit(info)


def test_phi():
    assert len(phi) == 52


def test_simple_commit(pod):
    # Create 5 changeset in series
    datum = b"ham spam foo bar baz".split()
    changelog = Changelog(pod)
    populate(changelog, datum)

    res = pod.ls()
    assert len(res) == len(datum)

    # Read commits
    revs = list(changelog.walk())
    for rev, expected in zip(revs, datum):
        assert rev.payload.startswith(hexdigest(expected))


def test_concurrent_commit(pod):
    datum = b"ham spam foo bar baz".split()
    changelogs = [Changelog(pod) for _ in range(len(datum))]
    contents = []
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = "Doe"
        info = f"{key} {timestamp} {author}".encode()
        contents.append(info)

    # Concurrent commits
    with ThreadPoolExecutor() as executor:
        futs = []
        for changelog, info in zip(changelogs, contents):
            f = executor.submit(changelog.commit, info.decode(), _jitter=True)
            futs.append(f)
        executor.shutdown()

    for f in futs:
        assert not f.exception()

    res = pod.ls()
    assert len(res) == len(datum)

    # As we inserted datum in a random fashion we have no order
    # garantee
    expected = set(map(hexdigest, datum))
    chlg = changelogs[0]
    chlg.refresh()
    for rev in chlg.walk():
        key, _ = rev.payload.split(" ", 1)
        expected.remove(key)
    assert not expected


def test_pack(pod):
    # Create 5 changeset in series
    datum = b"ham spam foo bar baz".split()
    changelog = Changelog(pod)
    populate(changelog, datum)

    changelog.pack()

    # Read commits
    revs = list(changelog.walk())
    for rev, expected in zip(revs, datum):
        assert rev.payload.startswith(hexdigest(expected))

def test_commit_object():
    parent = 'a-A'
    child = 'b-B'
    for ci in (Commit(parent, child), Commit.from_path('a-A.b-B')):
        assert ci.digests == ('A', 'B')
        assert ci.path == 'a-A.b-B'
