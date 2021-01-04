import time
from concurrent.futures import ThreadPoolExecutor

from lakota import Changelog
from lakota.changelog import Revision, phi
from lakota.pod import MemPOD
from lakota.utils import hexdigest

datum = b"ham spam foo bar baz".split()


def populate(changelog, datum):
    for data in datum:
        key = hexdigest(data)
        timestamp = 1234
        author = "Doe"
        info = f"{key} {timestamp} {author}"
        changelog.commit(info.encode())


def test_phi():
    assert len(phi) == 52


def test_simple_commit(pod):
    # Create 5 changeset in series
    changelog = Changelog(pod)
    populate(changelog, datum)

    res = pod.ls()
    assert len(res) == len(datum)

    # Read commits
    revs = list(changelog.log())
    for rev, expected in zip(revs, datum):
        payload = rev.read().decode()
        assert payload.startswith(hexdigest(expected))


def test_concurrent_commit(pod):
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
            f = executor.submit(changelog.commit, info, _jitter=True)
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
    for rev in chlg.log():
        payload = rev.read().decode()
        key, _ = payload.split(" ", 1)
        expected.remove(key)
    assert not expected


def test_commit_object():
    parent = "a-A"
    child = "b-B"
    changelog = None
    for ci in (
        Revision(changelog, parent, child),
        Revision.from_path(changelog, "a-A.b-B"),
    ):
        assert ci.digests == ("A", "B")
        assert ci.path == "a-A.b-B"


def test_leafs():
    # One new branch per commit
    changelog = Changelog(MemPOD("/"))
    for data in datum:
        changelog.commit(data, parents=[phi])
    assert len(changelog.leafs()) == len(datum)

    # 4 commits in two branches
    changelog = Changelog(MemPOD("/"))
    for data in [b"ham", b"spam"]:
        changelog.commit(data)

    # Sleep a bit, to make sure the 'foo/bar' branch wins
    time.sleep(0.01)

    (rev,) = changelog.commit(b"foo", parents=[phi])
    changelog.commit(b"bar", parents=[rev.child])
    leafs = changelog.leafs()
    assert len(leafs) == 2
    assert leafs[0].read() == b"spam"
    assert leafs[1].read() == b"bar"

    # Last writes wins
    assert changelog.leaf().read() == b"bar"
